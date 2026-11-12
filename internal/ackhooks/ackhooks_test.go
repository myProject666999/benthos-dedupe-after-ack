// Copyright 2025 Redpanda Data, Inc.

package ackhooks_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/internal/ackhooks"
	"github.com/redpanda-data/benthos/v4/internal/batch"
	"github.com/redpanda-data/benthos/v4/internal/message"
)

func TestEntryResolveSuccess(t *testing.T) {
	group, src := message.NewSortGroup(message.QuickBatch([][]byte{
		[]byte("a"), []byte("b"),
	}))
	entry := ackhooks.NewEntry(group, src)

	var gotA, gotB error
	entry.Register(0, func(err error) { gotA = err })
	entry.Register(1, func(err error) { gotB = err })

	entry.Resolve(nil)
	assert.NoError(t, gotA)
	assert.NoError(t, gotB)

	// Subsequent resolutions are no-ops.
	entry.Resolve(errors.New("ignored"))
	assert.NoError(t, gotA)
}

func TestEntryRegisterAfterResolve(t *testing.T) {
	group, src := message.NewSortGroup(message.QuickBatch([][]byte{[]byte("a")}))
	entry := ackhooks.NewEntry(group, src)

	sentinel := errors.New("boom")
	entry.Resolve(sentinel)

	var got error
	entry.Register(0, func(err error) { got = err })
	assert.Equal(t, sentinel, got, "late hook should fire immediately with recorded outcome")
}

func TestEntryGranularBatchErrorReduction(t *testing.T) {
	group, src := message.NewSortGroup(message.QuickBatch([][]byte{
		[]byte("k1"), []byte("k2"), []byte("k3"),
	}))
	entry := ackhooks.NewEntry(group, src)

	outcomes := map[int]error{}
	for i := range 3 {
		idx := i
		entry.Register(idx, func(err error) { outcomes[idx] = err })
	}

	// A granular failure for the middle message only.
	batchErr := batch.NewError(src, errors.New("general"))
	batchErr.Failed(1, errors.New("k2 failed"))
	entry.Resolve(batchErr)

	assert.NoError(t, outcomes[0], "k1 succeeded")
	assert.Error(t, outcomes[1], "k2 failed")
	assert.NoError(t, outcomes[2], "k3 succeeded")
}

func TestEntryNonBatchErrorAppliesToAll(t *testing.T) {
	group, src := message.NewSortGroup(message.QuickBatch([][]byte{
		[]byte("k1"), []byte("k2"),
	}))
	entry := ackhooks.NewEntry(group, src)

	outcomes := map[int]error{}
	for i := range 2 {
		idx := i
		entry.Register(idx, func(err error) { outcomes[idx] = err })
	}

	sentinel := errors.New("whole batch rejected")
	entry.Resolve(sentinel)
	assert.Equal(t, sentinel, outcomes[0])
	assert.Equal(t, sentinel, outcomes[1])
}

func TestSinkResolveAll(t *testing.T) {
	group, src := message.NewSortGroup(message.QuickBatch([][]byte{[]byte("a")}))
	sink := ackhooks.NewSink(group, src)
	assert.Same(t, group, sink.Group())
	assert.Equal(t, src, sink.SourceBatch())

	e1 := ackhooks.NewEntry(group, src)
	e2 := ackhooks.NewEntry(group, src)
	sink.Add(e1)
	sink.Add(e2)

	var fired1, fired2 bool
	e1.Register(0, func(error) { fired1 = true })
	e2.Register(0, func(error) { fired2 = true })

	sink.ResolveAll(nil)
	assert.True(t, fired1)
	assert.True(t, fired2)

	// Resolving again must not double-invoke hooks.
	sink.ResolveAll(errors.New("late"))
}

func TestSinkInContext(t *testing.T) {
	group, src := message.NewSortGroup(message.QuickBatch([][]byte{[]byte("a")}))
	sink := ackhooks.NewSink(group, src)

	p := message.NewPart([]byte("a"))
	p = message.WithContext(ackhooks.WithSink(message.GetContext(p), sink), p)

	require.Same(t, sink, ackhooks.SinkFromContext(message.GetContext(p)))
	assert.Nil(t, ackhooks.SinkFromContext(message.GetContext(message.NewPart(nil))))
}
