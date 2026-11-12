// Copyright 2025 Redpanda Data, Inc.

package pure

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/internal/ackhooks"
	"github.com/redpanda-data/benthos/v4/internal/bloblang/field"
	"github.com/redpanda-data/benthos/v4/internal/bundle"
	"github.com/redpanda-data/benthos/v4/internal/component"
	"github.com/redpanda-data/benthos/v4/internal/component/cache"
	"github.com/redpanda-data/benthos/v4/internal/component/interop"
	"github.com/redpanda-data/benthos/v4/internal/component/processor"
	"github.com/redpanda-data/benthos/v4/internal/log"
	"github.com/redpanda-data/benthos/v4/internal/message"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	dedupFieldCache          = "cache"
	dedupFieldKey            = "key"
	dedupFieldDropOnCacheErr = "drop_on_err"
	dedupFieldPostAck        = "post_ack"
	dedupFieldPendingTTL     = "pending_ttl"
)

// Cache value markers used by the post-ack deduplication mode.
const (
	dedupCommittedMarker = "t"
	dedupPendingPrefix   = "p:"
)

func dedupeProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary(`Deduplicates messages by storing a key value in a cache using the `+"`add`"+` operator. If the key already exists within the cache it is dropped.`).
		Description(`
Caches must be configured as resources, for more information check out the xref:components:caches/about.adoc[cache documentation].

When using this processor with an output target that might fail you should always wrap the output within an indefinite `+"xref:components:outputs/retry.adoc[`retry`]"+` block. This ensures that during outages your messages aren't reprocessed after failures, which would result in messages being dropped.

== Batch deduplication

This processor enacts on individual messages only, in order to perform a deduplication on behalf of a batch (or window) of messages instead use the `+"xref:components:processors/cache.adoc#examples[`cache` processor]"+`.

== Delivery guarantees

Performing deduplication on a stream using a distributed cache voids any at-least-once guarantees that it previously had. This is because the cache will preserve message signatures even if the message fails to leave the Redpanda Connect pipeline, which would cause message loss in the event of an outage at the output sink followed by a restart of the Redpanda Connect instance (or a server crash, etc).

This problem can be mitigated by using an in-memory cache and distributing messages to horizontally scaled Redpanda Connect pipelines partitioned by the deduplication key. However, in situations where at-least-once delivery guarantees are important it is worth avoiding deduplication in favour of implement idempotent behavior at the edge of your stream pipelines.

=== Post-acknowledgement deduplication

When the `+"`post_ack`"+` field is enabled the processor no longer considers a message processed as soon as it is seen. Instead it registers a short lived "processing" marker with the configured `+"`pending_ttl`"+`, and only commits the key once every message derived from it (including batches produced by processors such as `+"`split`"+`, `+"`archive`"+` or `+"`group_by`"+`) has been acknowledged successfully by the downstream output.

If the downstream output returns an error, or the transaction is cancelled, the processing marker is revoked so that a retried delivery of the same message is allowed through again. A concurrent duplicate arriving while the first message is still processing is dropped without affecting its acknowledgement. Should the process exit before the outcome is known the processing marker expires on its own via its TTL rather than blocking the key forever.

Because the decision is delayed until delivery acknowledgement the cache must support per-item TTLs (the `+"`memory`"+` cache does) in order to bound the lifetime of processing markers.`).
		Example(
			"Deduplicate based on Kafka key",
			"The following configuration demonstrates a pipeline that deduplicates messages based on the Kafka key.",
			`
pipeline:
  processors:
    - dedupe:
        cache: keycache
        key: ${! meta("kafka_key") }

cache_resources:
  - label: keycache
    memory:
      default_ttl: 60s
`,
		).
		Example(
			"Deduplicate only after successful delivery",
			"The following pipeline only marks a key as processed once the message has been acknowledged by the output, preserving at-least-once delivery when the output fails.",
			`
pipeline:
  processors:
    - dedupe:
        cache: keycache
        key: ${! meta("kafka_key") }
        post_ack:
          enabled: true
          pending_ttl: 30s

cache_resources:
  - label: keycache
    memory:
      default_ttl: 24h
`,
		).
		Fields(
			service.NewStringField(dedupFieldCache).
				Description("The xref:components:caches/about.adoc[`cache` resource] to target with this processor."),
			service.NewInterpolatedStringField(dedupFieldKey).
				Description(`An interpolated string yielding the key to deduplicate by for each message.`).
				Examples(`${! meta("kafka_key") }`, `${! content().hash("xxhash64") }`),
			service.NewBoolField(dedupFieldDropOnCacheErr).
				Description("Whether messages should be dropped when the cache returns a general error such as a network issue.").
				Default(true),
			service.NewObjectField(dedupFieldPostAck,
				service.NewBoolField("enabled").
					Description("Whether to defer committing deduplication keys until the derived messages have been acknowledged by the downstream output. When false the processor behaves exactly as it did without this field and marks keys immediately.").
					Default(false),
				service.NewDurationField(dedupFieldPendingTTL).
					Description("The TTL applied to processing markers registered while a message is in flight. The marker must outlive the worst case time between processing and delivery acknowledgement; if it expires early a concurrent duplicate may be allowed through.").
					Default("30s").
					Advanced(),
			).Description("Post-acknowledgement deduplication settings. When enabled a key is only considered processed after every message derived from it has been successfully delivered downstream.").Advanced(),
		)
}

func init() {
	service.MustRegisterBatchProcessor(
		"dedupe", dedupeProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			cache, err := conf.FieldString(dedupFieldCache)
			if err != nil {
				return nil, err
			}

			keyStr, err := conf.FieldString(dedupFieldKey)
			if err != nil {
				return nil, err
			}

			dropOnErr, err := conf.FieldBool(dedupFieldDropOnCacheErr)
			if err != nil {
				return nil, err
			}

			postAckEnabled, err := conf.FieldBool(dedupFieldPostAck, "enabled")
			if err != nil {
				return nil, err
			}

			pendingTTL, err := conf.FieldDuration(dedupFieldPostAck, dedupFieldPendingTTL)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newDedupe(cache, keyStr, dropOnErr, postAckEnabled, pendingTTL, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("dedupe", p, mgr)), nil
		})
}

type dedupeProc struct {
	log log.Modular

	dropOnErr  bool
	postAck    bool
	pendingTTL time.Duration
	key        *field.Expression
	mgr        bundle.NewManagement
	cacheName  string
}

func newDedupe(cache, keyStr string, dropOnErr, postAckEnabled bool, pendingTTL time.Duration, mgr bundle.NewManagement) (*dedupeProc, error) {
	if keyStr == "" {
		return nil, errors.New("dedupe key must not be empty")
	}
	key, err := mgr.BloblEnvironment().NewField(keyStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	if !mgr.ProbeCache(cache) {
		return nil, fmt.Errorf("cache resource '%v' was not found", cache)
	}

	if postAckEnabled && pendingTTL <= 0 {
		return nil, fmt.Errorf("dedupe %v.%v must be greater than zero when post-acknowledgement deduplication is enabled", dedupFieldPostAck, dedupFieldPendingTTL)
	}

	return &dedupeProc{
		log:        mgr.Logger(),
		dropOnErr:  dropOnErr,
		postAck:    postAckEnabled,
		pendingTTL: pendingTTL,
		key:        key,
		mgr:        mgr,
		cacheName:  cache,
	}, nil
}

func (d *dedupeProc) ProcessBatch(ctx *processor.BatchProcContext, batch message.Batch) ([]message.Batch, error) {
	if d.postAck {
		return d.processBatchPostAck(ctx, batch), nil
	}
	return d.processBatchLegacy(ctx, batch), nil
}

// processBatchLegacy preserves the original, immediate-marking behaviour. It is
// used whenever post-acknowledgement deduplication is disabled.
func (d *dedupeProc) processBatchLegacy(ctx *processor.BatchProcContext, batch message.Batch) []message.Batch {
	newBatch := message.QuickBatch(nil)
	_ = batch.Iter(func(i int, p *message.Part) error {
		key, err := d.key.String(i, batch)
		if err != nil {
			err = fmt.Errorf("key interpolation error: %w", err)
			ctx.OnError(err, i, nil)
			return nil
		}

		if cerr := d.mgr.AccessCache(context.Background(), d.cacheName, func(cache cache.V1) {
			err = cache.Add(context.Background(), key, []byte{'t'}, nil)
		}); cerr != nil {
			err = cerr
		}
		if err != nil {
			if errors.Is(err, component.ErrKeyAlreadyExists) {
				ctx.Span(i).LogKV("event", "dropped", "type", "deduplicated")
				return nil
			}

			d.log.Error("Cache error: %v\n", err)
			if d.dropOnErr {
				ctx.Span(i).LogKV("event", "dropped", "type", "deduplicated")
				return nil
			}

			ctx.OnError(err, i, p)
		}

		newBatch = append(newBatch, p)
		return nil
	})

	if newBatch.Len() == 0 {
		return nil
	}
	return []message.Batch{newBatch}
}

// processBatchPostAck registers a processing marker per accepted key and only
// commits the key once the derived messages are acknowledged downstream.
func (d *dedupeProc) processBatchPostAck(ctx *processor.BatchProcContext, batch message.Batch) []message.Batch {
	// Reuse the pipeline level sink so the entry settles against the same sort
	// group and index space as the source transaction, and so it still settles
	// if a later processor filters the derived messages. The sink is always
	// present inside a processing pipeline; fall back to a local entry for
	// direct processor use in tests.
	sink := ackhooks.SinkFromContext(message.GetContext(batch[0]))
	var entry *ackhooks.AckEntry
	if sink != nil {
		entry = ackhooks.NewEntry(sink.Group(), sink.SourceBatch())
		sink.Add(entry)
	} else {
		sortGroup, sortBatch := message.NewSortGroup(batch)
		entry = ackhooks.NewEntry(sortGroup, sortBatch)
	}

	newBatch := message.QuickBatch(nil)
	emitted := 0
	_ = batch.Iter(func(i int, p *message.Part) error {
		key, err := d.key.String(i, batch)
		if err != nil {
			err = fmt.Errorf("key interpolation error: %w", err)
			ctx.OnError(err, i, nil)
			return nil
		}

		token, err := d.addPendingMarker(key)
		if err != nil {
			if errors.Is(err, component.ErrKeyAlreadyExists) {
				// The key is either committed (already delivered) or being
				// processed by an in-flight message; either way drop this
				// duplicate without touching the other message's marker.
				ctx.Span(i).LogKV("event", "dropped", "type", "deduplicated")
				return nil
			}

			d.log.Error("Cache error: %v\n", err)
			if d.dropOnErr {
				ctx.Span(i).LogKV("event", "dropped", "type", "deduplicated")
				return nil
			}

			// The message continues without any marker, so its delivery
			// acknowledgement must not settle another message's key. Pass it on
			// with its original context untouched.
			ctx.OnError(err, i, p)
			newBatch = append(newBatch, p)
			emitted++
			return nil
		}

		// Settle the marker according to the final delivery outcome of this
		// specific source message.
		entry.Register(i, func(ackErr error) {
			if ackErr != nil {
				d.revokePendingMarker(key, token)
				return
			}
			d.commitKey(key, token)
		})
		newBatch = append(newBatch, p)
		emitted++
		return nil
	})

	if emitted == 0 {
		return nil
	}
	return []message.Batch{newBatch}
}

// addPendingMarker attempts to reserve the key with a short lived processing
// marker. A unique token is stored so that a later commit or revocation can
// verify it still owns the marker, which prevents a retried message from
// interfering with a newer in-flight attempt after the marker expired and was
// re-registered.
func (d *dedupeProc) addPendingMarker(key string) (string, error) {
	token, err := randomToken()
	if err != nil {
		return "", err
	}
	marker := dedupPendingPrefix + token

	ttl := d.pendingTTL
	if cerr := d.mgr.AccessCache(context.Background(), d.cacheName, func(c cache.V1) {
		err = c.Add(context.Background(), key, []byte(marker), &ttl)
	}); cerr != nil {
		return "", cerr
	}
	if err != nil {
		return "", err
	}
	return token, nil
}

// revokePendingMarker removes a processing marker, but only if the cache still
// holds this processor's exact marker. A missing or overwritten key means the
// marker already expired or a newer attempt owns it, in which case nothing is
// done.
func (d *dedupeProc) revokePendingMarker(key, token string) {
	expected := dedupPendingPrefix + token
	var delErr error
	if cerr := d.mgr.AccessCache(context.Background(), d.cacheName, func(c cache.V1) {
		value, err := c.Get(context.Background(), key)
		if err != nil {
			delErr = err
			return
		}
		if string(value) != expected {
			// The marker expired and another attempt took over, or the key was
			// committed. Leave it untouched.
			return
		}
		delErr = c.Delete(context.Background(), key)
	}); cerr != nil {
		d.log.Error("Failed to revoke processing marker for key %v: %v\n", key, cerr)
		return
	}
	if delErr != nil && !errors.Is(delErr, component.ErrKeyNotFound) {
		d.log.Error("Failed to revoke processing marker for key %v: %v\n", key, delErr)
	}
}

// commitKey turns a processing marker into a permanent processed marker, but
// only while this processor still owns the pending entry. If the marker
// expired and a newer attempt registered its own, the newer attempt wins and
// this acknowledgement is a no-op.
func (d *dedupeProc) commitKey(key, token string) {
	expected := dedupPendingPrefix + token
	var commitErr error
	if cerr := d.mgr.AccessCache(context.Background(), d.cacheName, func(c cache.V1) {
		value, err := c.Get(context.Background(), key)
		if err != nil {
			commitErr = err
			return
		}
		if string(value) != expected {
			// A newer in-flight attempt or an existing commit owns the key.
			return
		}
		commitErr = c.Set(context.Background(), key, []byte(dedupCommittedMarker), nil)
	}); cerr != nil {
		d.log.Error("Failed to commit deduplication key %v: %v\n", key, cerr)
		return
	}
	if commitErr != nil && !errors.Is(commitErr, component.ErrKeyNotFound) {
		d.log.Error("Failed to commit deduplication key %v: %v\n", key, commitErr)
	}
}

func randomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (d *dedupeProc) Close(context.Context) error {
	return nil
}
