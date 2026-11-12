// Copyright 2025 Redpanda Data, Inc.

// Package ackhooks provides an internal mechanism for processors to register
// acknowledgement hooks that are invoked once the final delivery outcome of
// their output messages is known.
//
// The processing pipeline creates an EntrySink per source transaction and
// places it into the context of the messages entering the processor chain. A
// processor such as dedupe creates an AckEntry, registers one hook per message
// it needs to settle and adds the entry to the sink. When the source
// transaction reaches its final outcome the pipeline resolves the sink, and
// each entry reduces the outcome down to the messages it owns by walking any
// granular batch errors against the pipeline level sort group tags that
// processors such as split, archive and group_by preserve.
package ackhooks

import (
	"context"
	"errors"
	"sync"

	"github.com/redpanda-data/benthos/v4/internal/batch"
	"github.com/redpanda-data/benthos/v4/internal/message"
)

type sinkCtxKey struct{}

// EntrySink is attached by the processing pipeline to the context of every
// message entering the processor chain. Processors register acknowledge
// entries with it so that they are settled when the source transaction reaches
// its final outcome, including entries whose messages are filtered before ever
// reaching an output transaction.
type EntrySink struct {
	// group and sourceBatch describe the pipeline level batch the sink was
	// created for. Entries reuse them to reduce granular batch errors against
	// the correct index space.
	group       *message.SortGroup
	sourceBatch message.Batch

	mu      sync.Mutex
	entries []*AckEntry
}

// NewSink creates a sink for a pipeline level source batch.
func NewSink(group *message.SortGroup, sourceBatch message.Batch) *EntrySink {
	return &EntrySink{group: group, sourceBatch: sourceBatch}
}

// Group returns the pipeline level sort group the sink was created for.
func (s *EntrySink) Group() *message.SortGroup { return s.group }

// SourceBatch returns the pipeline level source batch the sink was created
// for.
func (s *EntrySink) SourceBatch() message.Batch { return s.sourceBatch }

// WithSink returns a context carrying the provided entry sink.
func WithSink(ctx context.Context, sink *EntrySink) context.Context {
	return context.WithValue(ctx, sinkCtxKey{}, sink)
}

// SinkFromContext returns the entry sink carried by the context, if any.
func SinkFromContext(ctx context.Context) *EntrySink {
	sink, _ := ctx.Value(sinkCtxKey{}).(*EntrySink)
	return sink
}

// Add registers an entry with the sink.
func (s *EntrySink) Add(entry *AckEntry) {
	s.mu.Lock()
	s.entries = append(s.entries, entry)
	s.mu.Unlock()
}

// ResolveAll resolves every registered entry once with the provided error.
func (s *EntrySink) ResolveAll(err error) {
	s.mu.Lock()
	entries := s.entries
	s.entries = nil
	s.mu.Unlock()
	for _, entry := range entries {
		entry.Resolve(err)
	}
}

// Hook is invoked with the delivery outcome of the messages associated with a
// single registration. A nil error means that every derived message was
// acknowledged successfully, whereas a non-nil error means delivery failed.
type Hook func(err error)

// AckEntry aggregates acknowledgement hooks registered by a single processor
// invocation. An entry is safe for concurrent use: hooks may be registered from
// processing goroutines while a resolution may be triggered from the pipeline
// goroutine.
type AckEntry struct {
	group *message.SortGroup
	batch message.Batch

	mu       sync.Mutex
	hooks    map[int][]Hook
	resolved bool
	resErr   error
}

// NewEntry creates an acknowledgement entry that reduces errors against the
// provided pipeline level sort group and source batch.
func NewEntry(group *message.SortGroup, sourceBatch message.Batch) *AckEntry {
	return &AckEntry{
		group: group,
		batch: sourceBatch,
		hooks: map[int][]Hook{},
	}
}

// Register attaches a hook describing the outcome of the source message at the
// given pipeline level index. If the entry has already been resolved the hook
// is invoked immediately with the reduced outcome, which keeps processing
// correct when a transaction is acknowledged synchronously before the processor
// returns.
func (e *AckEntry) Register(sourceIndex int, h Hook) {
	e.mu.Lock()
	if e.resolved {
		err := e.reducedError(e.resErr, sourceIndex)
		e.mu.Unlock()
		h(err)
		return
	}
	e.hooks[sourceIndex] = append(e.hooks[sourceIndex], h)
	e.mu.Unlock()
}

// Resolve finalises the entry with the given delivery error and invokes every
// registered hook exactly once, reducing any granular batch error to the
// outcome of each hook's own source message. Subsequent calls are no-ops.
func (e *AckEntry) Resolve(err error) {
	e.mu.Lock()
	if e.resolved {
		e.mu.Unlock()
		return
	}
	e.resolved = true
	e.resErr = err
	hooks := e.hooks
	e.hooks = nil
	e.mu.Unlock()

	for index, indexHooks := range hooks {
		indexErr := e.reducedError(err, index)
		for _, h := range indexHooks {
			h(indexErr)
		}
	}
}

// reducedError narrows a transaction error down to the outcome of a single
// source message index, mirroring the reduction performed by
// transaction.Tracked for merged and split batches.
func (e *AckEntry) reducedError(err error, sourceIndex int) error {
	if err == nil || e.group == nil || e.batch == nil {
		return err
	}

	var walkable *batch.Error
	if !errors.As(err, &walkable) {
		return err
	}

	var res error
	found := false
	walkable.WalkPartsBySource(e.group, e.batch, func(index int, _ *message.Part, partErr error) bool {
		if index != sourceIndex {
			return true
		}
		found = true
		if partErr != nil {
			res = partErr
		}
		return false
	})
	if !found {
		// The source message is not represented in the granular error, so we
		// cannot prove success and must treat the batch as failed.
		return errors.Unwrap(walkable)
	}
	return res
}
