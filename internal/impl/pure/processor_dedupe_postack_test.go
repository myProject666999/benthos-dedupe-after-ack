// Copyright 2025 Redpanda Data, Inc.

package pure_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/internal/batch"
	"github.com/redpanda-data/benthos/v4/internal/component"
	"github.com/redpanda-data/benthos/v4/internal/component/cache"
	"github.com/redpanda-data/benthos/v4/internal/component/processor"
	"github.com/redpanda-data/benthos/v4/internal/component/testutil"
	"github.com/redpanda-data/benthos/v4/internal/manager/mock"
	"github.com/redpanda-data/benthos/v4/internal/message"
	"github.com/redpanda-data/benthos/v4/internal/pipeline"
)

// ttlCache is a cache.V1 implementation that honours per-item TTLs, unlike the
// map backed mock cache. It is used to verify that processing markers expire on
// their own when a process crash is simulated.
type ttlCache struct {
	mu    sync.Mutex
	items map[string]ttlCacheItem
}

type ttlCacheItem struct {
	value   []byte
	expires time.Time
}

func newTTLCache() *ttlCache {
	return &ttlCache{items: map[string]ttlCacheItem{}}
}

func (c *ttlCache) compactLocked() {
	now := time.Now()
	for k, v := range c.items {
		if !v.expires.IsZero() && v.expires.Before(now) {
			delete(c.items, k)
		}
	}
}

func (c *ttlCache) Get(_ context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compactLocked()
	v, ok := c.items[key]
	if !ok {
		return nil, component.ErrKeyNotFound
	}
	return v.value, nil
}

func (c *ttlCache) Set(_ context.Context, key string, value []byte, ttl *time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item := ttlCacheItem{value: value}
	if ttl != nil {
		item.expires = time.Now().Add(*ttl)
	}
	c.items[key] = item
	return nil
}

func (c *ttlCache) SetMulti(_ context.Context, items map[string]cache.TTLItem) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range items {
		item := ttlCacheItem{value: v.Value}
		if v.TTL != nil {
			item.expires = time.Now().Add(*v.TTL)
		}
		c.items[k] = item
	}
	return nil
}

func (c *ttlCache) Add(_ context.Context, key string, value []byte, ttl *time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compactLocked()
	if _, ok := c.items[key]; ok {
		return component.ErrKeyAlreadyExists
	}
	item := ttlCacheItem{value: value}
	if ttl != nil {
		item.expires = time.Now().Add(*ttl)
	}
	c.items[key] = item
	return nil
}

func (c *ttlCache) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; !ok {
		return component.ErrKeyNotFound
	}
	delete(c.items, key)
	return nil
}

func (c *ttlCache) Close(context.Context) error { return nil }

func (c *ttlCache) snapshot() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compactLocked()
	out := make(map[string]string, len(c.items))
	for k, v := range c.items {
		out[k] = string(v.value)
	}
	return out
}

// failingCache can be toggled to return general cache errors.
type failingCache struct {
	cache.V1
	fail bool
}

func (c *failingCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if c.fail {
		return errors.New("cache unavailable")
	}
	return c.V1.Add(ctx, key, value, ttl)
}

func (c *failingCache) Get(ctx context.Context, key string) ([]byte, error) {
	if c.fail {
		return nil, errors.New("cache unavailable")
	}
	return c.V1.Get(ctx, key)
}

func (c *failingCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if c.fail {
		return errors.New("cache unavailable")
	}
	return c.V1.Set(ctx, key, value, ttl)
}

func (c *failingCache) Delete(ctx context.Context, key string) error {
	if c.fail {
		return errors.New("cache unavailable")
	}
	return c.V1.Delete(ctx, key)
}

const postAckDedupeYAML = `
dedupe:
  cache: foocache
  key: ${! content() }
  post_ack:
    enabled: true
    pending_ttl: 30s
`

func newPostAckDedupe(t *testing.T, mgr *mock.Manager) processor.V1 {
	t.Helper()
	conf, err := testutil.ProcessorFromYAML(postAckDedupeYAML)
	require.NoError(t, err)
	proc, err := mgr.NewProcessor(conf)
	require.NoError(t, err)
	return proc
}

// dedupeHarness drives a processing pipeline directly with transactions,
// mimicking the input -> processors -> output wiring without external services.
type dedupeHarness struct {
	t    *testing.T
	pipe *pipeline.Processor
	in   chan message.Transaction
	// ackCtx is an independent context used for acknowledgements. It must not
	// be the test context because the pipeline cleanup cancels that, which
	// would surface as spurious "context canceled" ack errors.
	ackCtx    context.Context
	ackCancel context.CancelFunc
}

func newDedupeHarness(t *testing.T, procs ...processor.V1) *dedupeHarness {
	t.Helper()
	pipe := pipeline.NewProcessor(false, procs...)
	pipe.NoCloseProcessorsForTest()
	in := make(chan message.Transaction)
	require.NoError(t, pipe.Consume(in))
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		ackCancel()
		pipe.TriggerCloseNow()
		assert.NoError(t, pipe.WaitForClose(context.Background()))
	})
	return &dedupeHarness{t: t, pipe: pipe, in: in, ackCtx: ackCtx, ackCancel: ackCancel}
}

// ack acknowledges a downstream transaction using the harness ack context.
func (h *dedupeHarness) ack(tran message.Transaction, err error) {
	h.t.Helper()
	require.NoError(h.t, tran.Ack(h.ackCtx, err))
}

// send pushes a batch into the pipeline as a source transaction and returns the
// transaction emitted downstream (with its ack func) together with the source
// response channel. It fails the test if the pipeline filters the whole batch;
// use sendFiltered for that expectation.
func (h *dedupeHarness) send(parts ...string) (message.Transaction, chan error) {
	h.t.Helper()
	bytes := make([][]byte, len(parts))
	for i, p := range parts {
		bytes[i] = []byte(p)
	}
	resChan := make(chan error, 1)
	h.in <- message.NewTransaction(message.QuickBatch(bytes), resChan)

	select {
	case tran := <-h.pipe.TransactionChan():
		return tran, resChan
	case err := <-resChan:
		h.t.Fatalf("expected batch to propagate, it was resolved with: %v", err)
		return message.Transaction{}, nil
	case <-time.After(2 * time.Second):
		h.t.Fatal("timed out waiting for pipeline output")
		return message.Transaction{}, nil
	}
}

// sendFiltered sends a batch that is expected to be filtered entirely, and
// returns the source ack error.
func (h *dedupeHarness) sendFiltered(parts ...string) error {
	h.t.Helper()
	bytes := make([][]byte, len(parts))
	for i, p := range parts {
		bytes[i] = []byte(p)
	}
	resChan := make(chan error, 1)
	h.in <- message.NewTransaction(message.QuickBatch(bytes), resChan)
	select {
	case tran := <-h.pipe.TransactionChan():
		h.t.Fatalf("expected batch to be filtered, got %d parts", tran.Payload.Len())
		return nil
	case err := <-resChan:
		return err
	case <-time.After(2 * time.Second):
		h.t.Fatal("timed out waiting for filtered batch")
		return nil
	}
}

// waitAck reads the source acknowledgement, failing the test on timeout.
func (h *dedupeHarness) waitAck(resChan chan error) error {
	h.t.Helper()
	select {
	case err := <-resChan:
		return err
	case <-time.After(2 * time.Second):
		h.t.Fatal("timed out waiting for source acknowledgement")
		return nil
	}
}

// TestDedupePostAckCommit verifies that a key is only committed after the
// downstream output acknowledges the message.
func TestDedupePostAckCommit(t *testing.T) {
	mgr := mock.NewManager()
	c := newTTLCache()
	mgr.RegisterCacheForTest("foocache", c)
	h := newDedupeHarness(t, newPostAckDedupe(t, mgr))

	tran, resChan := h.send("doc-a")

	// Before acknowledgement the key must only have a processing marker.
	values := c.snapshot()
	require.Contains(t, values, "doc-a")
	assert.True(t, strings.HasPrefix(values["doc-a"], "p:"), "expected pending marker for doc-a")

	// A duplicate arriving while the first is in flight is dropped, and its
	// filtering does not disturb the in-flight marker.
	assert.NoError(t, h.sendFiltered("doc-a"))
	assert.Len(t, c.snapshot(), 1)

	// Downstream acknowledges successfully: the key becomes committed.
	h.ack(tran, nil)
	assert.NoError(t, h.waitAck(resChan))

	values = c.snapshot()
	require.Contains(t, values, "doc-a")
	assert.Equal(t, "t", values["doc-a"])

	// Now a redelivery of the same message is deduplicated.
	assert.NoError(t, h.sendFiltered("doc-a"))
}

// TestDedupePostAckNackRevokes verifies that a failed delivery revokes the
// processing marker so the retried message passes through again, and is then
// committed once delivery succeeds.
func TestDedupePostAckNackRevokes(t *testing.T) {
	mgr := mock.NewManager()
	c := newTTLCache()
	mgr.RegisterCacheForTest("foocache", c)
	h := newDedupeHarness(t, newPostAckDedupe(t, mgr))

	tran, resChan := h.send("doc-b")
	require.Contains(t, c.snapshot(), "doc-b")

	// Simulate an output failure (e.g. reject output).
	outErr := errors.New("output rejected")
	h.ack(tran, outErr)
	assert.Equal(t, outErr, h.waitAck(resChan))

	// Marker must have been revoked.
	assert.NotContains(t, c.snapshot(), "doc-b")

	// The retried delivery passes through again.
	retryTran, retryRes := h.send("doc-b")
	require.Contains(t, c.snapshot(), "doc-b")

	h.ack(retryTran, nil)
	assert.NoError(t, h.waitAck(retryRes))
	assert.Equal(t, "t", c.snapshot()["doc-b"])
}

// TestDedupePostAckPartialFailure verifies that when a merged batch partially
// fails only the keys of the failed source messages are revoked while the
// successful ones are committed.
func TestDedupePostAckPartialFailure(t *testing.T) {
	mgr := mock.NewManager()
	c := newTTLCache()
	mgr.RegisterCacheForTest("foocache", c)
	h := newDedupeHarness(t, newPostAckDedupe(t, mgr))

	tran, resChan := h.send("k1", "k2", "k3")
	require.Len(t, tran.Payload, 3)

	// Simulate an output batcher reporting a granular failure for only the
	// message derived from k2.
	sortGroup, sortedBatch := message.NewSortGroup(tran.Payload)
	batchErr := batch.NewError(sortedBatch, errors.New("partial write failure"))
	idx := sortGroup.GetIndex(sortedBatch[1])
	require.GreaterOrEqual(t, idx, 0)
	batchErr.Failed(idx, errors.New("k2 write failed"))

	h.ack(tran, batchErr)
	assert.Error(t, h.waitAck(resChan))

	values := c.snapshot()
	assert.Equal(t, "t", values["k1"], "successful message should be committed")
	assert.NotContains(t, values, "k2", "failed message marker should be revoked")
	assert.Equal(t, "t", values["k3"], "successful message should be committed")

	// The revoked key can flow through again on retry and then commit.
	retryTran, retryRes := h.send("k2")
	h.ack(retryTran, nil)
	assert.NoError(t, h.waitAck(retryRes))
	assert.Equal(t, "t", c.snapshot()["k2"])
}

// TestDedupePostAckSplitBatches verifies that when a batch fans out into
// multiple output transactions each key is only committed once the derived
// batch it belongs to succeeds, and a failure of any derived batch revokes the
// corresponding key.
func TestDedupePostAckSplitBatches(t *testing.T) {
	mgr := mock.NewManager()
	c := newTTLCache()
	mgr.RegisterCacheForTest("foocache", c)

	splitConf, err := testutil.ProcessorFromYAML(`
split:
  size: 1
`)
	require.NoError(t, err)
	splitProc, err := mgr.NewProcessor(splitConf)
	require.NoError(t, err)

	h := newDedupeHarness(t, newPostAckDedupe(t, mgr), splitProc)

	resChan := make(chan error, 1)
	h.in <- message.NewTransaction(
		message.QuickBatch([][]byte{[]byte("k1"), []byte("k2")}), resChan)

	// Two source messages split into two output transactions.
	var trans []message.Transaction
	for range 2 {
		select {
		case tran := <-h.pipe.TransactionChan():
			trans = append(trans, tran)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for split transaction")
		}
	}
	require.Len(t, trans, 2)

	values := c.snapshot()
	assert.True(t, strings.HasPrefix(values["k1"], "p:"), "expected pending marker for k1")
	assert.True(t, strings.HasPrefix(values["k2"], "p:"), "expected pending marker for k2")

	// While any derived batch is outstanding neither key may settle.
	h.ack(trans[0], nil)
	assert.True(t, strings.HasPrefix(c.snapshot()[firstKey(trans[0])], "p:"),
		"key stays pending until every derived batch is acknowledged")

	// The second derived batch fails. Once every derived batch has settled the
	// source transaction is nacked, and the granular error only revokes the
	// failed key while the successful one commits.
	failErr := errors.New("split batch failed")
	h.ack(trans[1], failErr)
	assert.Error(t, h.waitAck(resChan))

	values = c.snapshot()
	require.Len(t, values, 1, "only the committed key should remain")
	for _, v := range values {
		assert.Equal(t, "t", v)
	}
}

// firstKey extracts the deduplication key (the original content) from a
// transaction payload, used to identify which source message a split batch
// belongs to.
func firstKey(tran message.Transaction) string {
	return string(tran.Payload.Get(0).AsBytes())
}

// TestDedupePostAckArchiveMergeFailure verifies that when an archive processor
// merges several source messages into one and the merged write fails, every
// source message's marker is revoked (the whole derived set must succeed for
// any key to commit).
func TestDedupePostAckArchiveMergeFailure(t *testing.T) {
	mgr := mock.NewManager()
	c := newTTLCache()
	mgr.RegisterCacheForTest("foocache", c)

	archiveConf, err := testutil.ProcessorFromYAML(`
archive:
  format: lines
`)
	require.NoError(t, err)
	archiveProc, err := mgr.NewProcessor(archiveConf)
	require.NoError(t, err)

	h := newDedupeHarness(t, newPostAckDedupe(t, mgr), archiveProc)

	resChan := make(chan error, 1)
	h.in <- message.NewTransaction(
		message.QuickBatch([][]byte{[]byte("m1"), []byte("m2")}), resChan)

	var tran message.Transaction
	select {
	case tran = <-h.pipe.TransactionChan():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for archived transaction")
	}
	// The two messages were collapsed into a single archived part.
	require.Len(t, tran.Payload, 1)

	// A non-granular failure of the merged write must revoke every key.
	h.ack(tran, errors.New("archive sink unavailable"))
	assert.Error(t, h.waitAck(resChan))

	values := c.snapshot()
	assert.NotContains(t, values, "m1", "merged-set failure revokes m1")
	assert.NotContains(t, values, "m2", "merged-set failure revokes m2")

	// Both messages can be redelivered after revocation.
	retryTran, retryRes := h.send("m1", "m2")
	require.Len(t, retryTran.Payload, 1)
	h.ack(retryTran, nil)
	assert.NoError(t, h.waitAck(retryRes))
	values = c.snapshot()
	assert.Equal(t, "t", values["m1"])
	assert.Equal(t, "t", values["m2"])
}

// TestDedupePostAckConcurrentDuplicates hammers the processor with concurrent
// deliveries of the same key and asserts that exactly one is accepted, while
// every other delivery is filtered and the winner can commit normally.
func TestDedupePostAckConcurrentDuplicates(t *testing.T) {
	mgr := mock.NewManager()
	c := newTTLCache()
	mgr.RegisterCacheForTest("foocache", c)
	h := newDedupeHarness(t, newPostAckDedupe(t, mgr))

	const n = 20
	var wg sync.WaitGroup
	accepted := make(chan message.Transaction, n)
	dropped := make(chan struct{}, n)

	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resChan := make(chan error, 1)
			h.in <- message.NewTransaction(
				message.QuickBatch([][]byte{[]byte("hot-key")}), resChan)
			select {
			case tran, ok := <-h.pipe.TransactionChan():
				if ok {
					accepted <- tran
					return
				}
			case err := <-resChan:
				assert.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Error("timed out waiting for delivery outcome")
				return
			}
			dropped <- struct{}{}
		}()
	}

	var winner message.Transaction
	select {
	case winner = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("no message was accepted")
	}

	// Acknowledge the winner; the remaining deliveries must all be filtered.
	h.ack(winner, nil)
	wg.Wait()
	close(accepted)
	close(dropped)

	assert.Empty(t, accepted)
	assert.Len(t, dropped, n-1)
	assert.Equal(t, "t", c.snapshot()["hot-key"])
}

// TestDedupePostAckPendingTTLExpiry simulates a process crash by never
// acknowledging the in-flight message and waiting for the processing marker to
// expire, after which a redelivery is accepted again.
func TestDedupePostAckPendingTTLExpiry(t *testing.T) {
	mgr := mock.NewManager()
	c := newTTLCache()
	mgr.RegisterCacheForTest("foocache", c)

	conf, err := testutil.ProcessorFromYAML(`
dedupe:
  cache: foocache
  key: ${! content() }
  post_ack:
    enabled: true
    pending_ttl: 60ms
`)
	require.NoError(t, err)
	proc, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	h := newDedupeHarness(t, proc)
	tran, _ := h.send("crash-key")
	require.Contains(t, c.snapshot(), "crash-key")

	// Simulate a crash: never acknowledge. Wait for the TTL to pass.
	require.Eventually(t, func() bool {
		_, ok := c.snapshot()["crash-key"]
		return !ok
	}, time.Second, 10*time.Millisecond)

	// A new pipeline instance accepts the key again and commits on ack.
	proc2, err := mgr.NewProcessor(conf)
	require.NoError(t, err)
	h2 := newDedupeHarness(t, proc2)
	tran2, resChan2 := h2.send("crash-key")
	h2.ack(tran2, nil)
	assert.NoError(t, h2.waitAck(resChan2))
	assert.Equal(t, "t", c.snapshot()["crash-key"])

	_ = tran
}

// TestDedupePostAckCacheErrors verifies the drop_on_err behaviour in post-ack
// mode.
func TestDedupePostAckCacheErrors(t *testing.T) {
	mgr := mock.NewManager()
	fc := &failingCache{V1: newTTLCache(), fail: true}
	mgr.RegisterCacheForTest("foocache", fc)

	// Default drop_on_err: true -> messages are dropped on cache errors.
	proc := newPostAckDedupe(t, mgr)
	msgs, err := proc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("a"), []byte("b")}))
	require.NoError(t, err)
	assert.Empty(t, msgs)

	// drop_on_err: false -> messages pass through without markers.
	conf, err := testutil.ProcessorFromYAML(`
dedupe:
  cache: foocache
  key: ${! content() }
  drop_on_err: false
  post_ack:
    enabled: true
    pending_ttl: 30s
`)
	require.NoError(t, err)
	proc, err = mgr.NewProcessor(conf)
	require.NoError(t, err)

	msgs, err = proc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("a"), []byte("b")}))
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Len(t, msgs[0], 2)
}

// TestDedupePostAckDisabledByDefault verifies that omitting the post_ack block
// keeps the original immediate-marking behaviour, including its marker shape.
func TestDedupePostAckDisabledByDefault(t *testing.T) {
	mgr := mock.NewManager()
	c := newTTLCache()
	mgr.RegisterCacheForTest("foocache", c)

	conf, err := testutil.ProcessorFromYAML(`
dedupe:
  cache: foocache
  key: ${! content() }
`)
	require.NoError(t, err)
	proc, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	msgs, err := proc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("legacy")}))
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "t", c.snapshot()["legacy"])

	// No ack hook should be attached: even a nack leaves the committed marker.
	tran := message.NewTransaction(msgs[0], make(chan error, 1))
	require.NoError(t, tran.Ack(t.Context(), errors.New("nack leaves the legacy marker untouched")))
	assert.Equal(t, "t", c.snapshot()["legacy"])
}

// TestDedupePostAckFilteredMessage covers a message that passes dedupe but is
// then filtered by a later processor: its marker is settled as successful
// rather than waiting for TTL expiry.
func TestDedupePostAckFilteredMessage(t *testing.T) {
	mgr := mock.NewManager()
	c := newTTLCache()
	mgr.RegisterCacheForTest("foocache", c)

	dropConf, err := testutil.ProcessorFromYAML(`
mapping: 'root = deleted()'
`)
	require.NoError(t, err)
	dropProc, err := mgr.NewProcessor(dropConf)
	require.NoError(t, err)

	h := newDedupeHarness(t, newPostAckDedupe(t, mgr), dropProc)
	assert.NoError(t, h.sendFiltered("filtered-key"))

	// Deliberate filtering counts as a successful outcome so the key commits.
	assert.Equal(t, "t", c.snapshot()["filtered-key"])
}

// TestDedupePostAckInvalidTTL ensures a non-positive pending TTL is rejected.
func TestDedupePostAckInvalidTTL(t *testing.T) {
	mgr := mock.NewManager()
	mgr.RegisterCacheForTest("foocache", newTTLCache())

	conf, err := testutil.ProcessorFromYAML(`
dedupe:
  cache: foocache
  key: ${! content() }
  post_ack:
    enabled: true
    pending_ttl: 0s
`)
	require.NoError(t, err)
	_, err = mgr.NewProcessor(conf)
	require.Error(t, err)
}
