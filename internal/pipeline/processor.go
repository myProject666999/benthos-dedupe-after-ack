// Copyright 2025 Redpanda Data, Inc.

package pipeline

import (
	"context"
	"sync"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/internal/ackhooks"
	"github.com/redpanda-data/benthos/v4/internal/batch"
	"github.com/redpanda-data/benthos/v4/internal/component"
	"github.com/redpanda-data/benthos/v4/internal/component/processor"
	"github.com/redpanda-data/benthos/v4/internal/message"
)

// Processor is a pipeline that supports both Consumer and Producer interfaces.
// The processor will read from a source, perform some processing, and then
// either propagate a new message or drop it.
type Processor struct {
	noCloseProcs  bool
	strict        bool
	msgProcessors []processor.V1

	messagesOut chan message.Transaction
	responsesIn chan error

	messagesIn <-chan message.Transaction

	shutSig *shutdown.Signaller
}

// NewProcessor returns a new message processing pipeline. When strict is true
// the pipeline applies strict error handling: a message that fails a processor
// skips the remaining processors and is rejected at the output.
func NewProcessor(strict bool, msgProcessors ...processor.V1) *Processor {
	return &Processor{
		strict:        strict,
		msgProcessors: msgProcessors,
		messagesOut:   make(chan message.Transaction),
		responsesIn:   make(chan error),
		shutSig:       shutdown.NewSignaller(),
	}
}

//------------------------------------------------------------------------------

// loop is the processing loop of this pipeline.
func (p *Processor) loop() {
	closeNowCtx, cnDone := p.shutSig.HardStopCtx(context.Background())
	defer cnDone()

	if p.strict {
		closeNowCtx = processor.WithStrict(closeNowCtx)
	}

	defer func() {
		if !p.noCloseProcs {
			for _, c := range p.msgProcessors {
				if err := c.Close(closeNowCtx); err != nil {
					break
				}
			}
		}

		close(p.messagesOut)
		p.shutSig.TriggerHasStopped()
	}()

	var open bool
	for !p.shutSig.IsSoftStopSignalled() {
		var tran message.Transaction
		select {
		case tran, open = <-p.messagesIn:
			if !open {
				return
			}
		case <-p.shutSig.HardStopChan():
			return
		}

		sorter, sortBatch := message.NewSortGroup(tran.Payload)

		// Attach a sink that processors such as dedupe use to register
		// acknowledgement entries. Because the AutoObserved processor wrapper
		// restores pre-processing contexts onto output parts, entries cannot be
		// propagated on the parts themselves; the sink is instead settled
		// directly here when the source transaction reaches its final outcome.
		sink := ackhooks.NewSink(sorter, sortBatch)
		_ = sortBatch.Iter(func(i int, p *message.Part) error {
			sortBatch[i] = message.WithContext(ackhooks.WithSink(message.GetContext(p), sink), p)
			return nil
		})

		// settleSink resolves every registered entry with the transaction's
		// final delivery error. It is idempotent so it is safe to invoke from
		// both the filtered/error branch and the normal acknowledgement path.
		sinkSettled := false
		settleSink := func(ackErr error) {
			if sinkSettled {
				return
			}
			sinkSettled = true
			sink.ResolveAll(ackErr)
		}

		resultBatches, err := processor.ExecuteAll(closeNowCtx, p.msgProcessors, sortBatch)
		if len(resultBatches) == 0 || err != nil {
			// An empty result means every message was deliberately filtered,
			// which counts as a successful outcome for the registered entries;
			// an execution error must instead be retried.
			settleSink(err)
			if _ = tran.Ack(closeNowCtx, err); closeNowCtx.Err() != nil {
				return
			}
			continue
		}

		if len(resultBatches) == 1 {
			select {
			case p.messagesOut <- message.NewTransactionFunc(resultBatches[0], func(ctx context.Context, ackErr error) error {
				settleSink(ackErr)
				return tran.Ack(ctx, ackErr)
			}):
			case <-p.shutSig.HardStopChan():
				return
			}
			continue
		}

		var (
			errMut     sync.Mutex
			batchErr   *batch.Error
			generalErr error
			batchWG    sync.WaitGroup
		)

		for _, b := range resultBatches {
			var wgOnce sync.Once
			batchWG.Add(1)
			tmpBatch := b.ShallowCopy()

			select {
			case p.messagesOut <- message.NewTransactionFunc(tmpBatch, func(ctx context.Context, err error) error {
				if err != nil {
					errMut.Lock()
					defer errMut.Unlock()

					if batchErr == nil {
						batchErr = batch.NewError(sortBatch, err)
					}
					for _, m := range tmpBatch {
						if bIndex := sorter.GetIndex(m); bIndex >= 0 {
							batchErr.Failed(bIndex, err)
						} else {
							// We are unable to link this message with an origin
							// and therefore we must provide a general
							// batch-wide error instead.
							generalErr = err
						}
					}
				}

				wgOnce.Do(func() {
					batchWG.Done()
				})
				return nil
			}):
			case <-p.shutSig.HardStopChan():
				return
			}
		}

		batchWG.Wait()

		if generalErr != nil {
			settleSink(generalErr)
			_ = tran.Ack(closeNowCtx, generalErr)
		} else if batchErr != nil {
			settleSink(batchErr)
			_ = tran.Ack(closeNowCtx, batchErr)
		} else {
			settleSink(nil)
			_ = tran.Ack(closeNowCtx, nil)
		}
	}
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the pipeline to read.
func (p *Processor) Consume(msgs <-chan message.Transaction) error {
	if p.messagesIn != nil {
		return component.ErrAlreadyStarted
	}
	p.messagesIn = msgs
	go p.loop()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// pipeline.
func (p *Processor) TransactionChan() <-chan message.Transaction {
	return p.messagesOut
}

// TriggerCloseNow signals that the processor pipeline should close immediately.
func (p *Processor) TriggerCloseNow() {
	p.shutSig.TriggerHardStop()
}

// NoCloseProcessorsForTest prevents the pipeline from closing the processors
// that were shared with another pipeline (or managed externally), mirroring the
// behaviour used by NewPool. It is intended for tests that drive a pipeline
// directly.
func (p *Processor) NoCloseProcessorsForTest() {
	p.noCloseProcs = true
}

// WaitForClose blocks until the component has closed down or the context is
// cancelled. Closing occurs either when the input transaction channel is closed
// and messages are flushed (and acked), or when CloseNowAsync is called.
func (p *Processor) WaitForClose(ctx context.Context) error {
	select {
	case <-p.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
