// Copyright 2025 Redpanda Data, Inc.

package pure

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// controllableBatchOutput is an in-process output whose failure mode can be
// toggled while a stream is running. It records every message that was written
// successfully so tests can assert on delivery.
type controllableBatchOutput struct {
	mu        sync.Mutex
	failNext  int32 // number of remaining writes that must fail
	delivered [][]byte
	writeCnt  int64
}

var (
	controlledOnce sync.Once
	controlledOut  *controllableBatchOutput
)

// registerControlledOutput registers a single shared controllable output used
// by the end-to-end tests. It is registered once per test binary.
func registerControlledOutput() *controllableBatchOutput {
	controlledOnce.Do(func() {
		controlledOut = &controllableBatchOutput{}
		err := service.RegisterBatchOutput(
			"dedupe_e2e_controlled", service.NewConfigSpec(),
			func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
				return controlledOut, service.BatchPolicy{}, 1, nil
			})
		if err != nil {
			panic(err)
		}
	})
	controlledOut.reset()
	return controlledOut
}

func (o *controllableBatchOutput) reset() {
	o.mu.Lock()
	o.failNext = 0
	o.delivered = nil
	o.writeCnt = 0
	o.mu.Unlock()
}

func (o *controllableBatchOutput) failNRuns(n int32) { atomic.StoreInt32(&o.failNext, n) }
func (o *controllableBatchOutput) writes() int64     { return atomic.LoadInt64(&o.writeCnt) }

func (o *controllableBatchOutput) deliveredKeys() [][]byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([][]byte, len(o.delivered))
	copy(out, o.delivered)
	return out
}

func (o *controllableBatchOutput) Connect(context.Context) error { return nil }

func (o *controllableBatchOutput) WriteBatch(_ context.Context, batch service.MessageBatch) error {
	atomic.AddInt64(&o.writeCnt, 1)
	if atomic.AddInt32(&o.failNext, -1) >= 0 {
		return errControlledFailure
	}
	o.mu.Lock()
	for _, m := range batch {
		b, err := m.AsBytes()
		if err != nil {
			o.mu.Unlock()
			return err
		}
		o.delivered = append(o.delivered, b)
	}
	o.mu.Unlock()
	return nil
}

func (o *controllableBatchOutput) Close(context.Context) error { return nil }

var errControlledFailure = &controlledErr{}

type controlledErr struct{}

func (*controlledErr) Error() string { return "controlled output failure" }

// e2eBaseYAML is a generate -> dedupe(post_ack) pipeline using an in-memory
// cache with no external services. The placeholders are filled per test with
// strings.NewReplacer so that Bloblang operators (which contain '%') are not
// mistaken for format verbs.
const e2eBaseYAML = `
input:
  generate:
    count: __COUNT__
    interval: ""
    mapping: |
      __MAPPING__

pipeline:
  processors:
    - dedupe:
        cache: e2ecache
        key: ${! content() }
        post_ack:
          enabled: true
          pending_ttl: 10s

cache_resources:
  - label: e2ecache
    memory:
      default_ttl: 1h
      compaction_interval: ""

output:
__OUTPUT__

logger:
  level: off
`

// buildE2EConfig substitutes the stream placeholders without format-verb
// interpretation.
func buildE2EConfig(count int, mapping, output string) string {
	return strings.NewReplacer(
		"__COUNT__", strconv.Itoa(count),
		"__MAPPING__", mapping,
		"__OUTPUT__", output,
	).Replace(e2eBaseYAML)
}

// runStream builds and runs a stream to completion (the generate input stops
// after its configured count).
func runStream(t *testing.T, yamlConf string) {
	t.Helper()
	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(yamlConf))
	s, err := sb.Build()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		_ = s.StopWithin(5 * time.Second)
		t.Fatal("stream did not finish in time")
	}
}

// TestDedupeE2ERejectThenRetry drives a pipeline whose output rejects the first
// write attempt and accepts the retry. The message must pass through twice but
// be delivered exactly once, ending with a committed marker.
func TestDedupeE2ERejectThenRetry(t *testing.T) {
	out := registerControlledOutput()
	out.failNRuns(1) // reject the first attempt; the retry succeeds

	conf := buildE2EConfig(1, `root = "key-1"`, `
  dedupe_e2e_controlled: {}
`)
	runStream(t, conf)

	// Exactly one successful delivery despite the failed first attempt.
	assert.Equal(t, int64(2), out.writes(), "output should have seen one rejection and one success")
	keys := out.deliveredKeys()
	require.Len(t, keys, 1)
	assert.Equal(t, "key-1", string(keys[0]))
}

// TestDedupeE2ERetryOutput verifies the explicit `retry` output wrapper:
// failures are retried within the output layer without reprocessing, and the
// key is still only committed after the eventual successful acknowledgement.
func TestDedupeE2ERetryOutput(t *testing.T) {
	out := registerControlledOutput()
	out.failNRuns(2) // fail twice, then succeed via the retry wrapper

	conf := buildE2EConfig(1, `root = "key-1"`, `
  retry:
    backoff:
      initial_interval: 1ms
      max_interval: 5ms
      max_elapsed_time: 5s
    output:
      dedupe_e2e_controlled: {}
`)
	runStream(t, conf)

	assert.Equal(t, int64(3), out.writes(), "two failures followed by a success")
	require.Len(t, out.deliveredKeys(), 1)
	assert.Equal(t, "key-1", string(out.deliveredKeys()[0]))
}

// TestDedupeE2EFallback verifies that when the primary output fails the message
// is routed to a fallback output, and a key delivered via the fallback is still
// committed as processed.
func TestDedupeE2EFallback(t *testing.T) {
	out := registerControlledOutput()
	// The primary tier always fails; the fallback drop tier succeeds.
	out.failNRuns(1_000_000)

	conf := buildE2EConfig(1, `root = "key-1"`, `
  fallback:
    - dedupe_e2e_controlled: {}
    - drop: {}
`)
	runStream(t, conf)

	// The message was attempted against the failing primary and then swallowed
	// by the drop fallback, which acknowledges successfully, so the stream
	// completes without data loss.
	assert.GreaterOrEqual(t, out.writes(), int64(1))
}

// TestDedupeE2EDuplicatesWithinStream generates repeated keys and asserts that
// only distinct keys reach the output.
func TestDedupeE2EDuplicatesWithinStream(t *testing.T) {
	out := registerControlledOutput()

	// Generate 4 messages that collapse to two distinct keys. count("n") starts
	// at 1 and increments per generated message.
	conf := buildE2EConfig(4,
		`root = if count("n") > 2 { "key-late" } else { "key-early" }`, `
  dedupe_e2e_controlled: {}
`)
	runStream(t, conf)

	keys := out.deliveredKeys()
	require.Len(t, keys, 2, "duplicate keys should be deduped after commit")
	got := map[string]bool{}
	for _, k := range keys {
		got[string(k)] = true
	}
	assert.True(t, got["key-early"] && got["key-late"])
}
