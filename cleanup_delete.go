package sqsconsumer

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/net/context"
)

type deleteQueue struct {
	queue chan *sqs.Message
	sync.Mutex
	entries []*sqs.DeleteMessageBatchRequestEntry

	svc                 *SQSService
	drainTimeout        time.Duration
	accumulationTimeout time.Duration
}

// NewBatchDeleter starts a batch deleter routine that deletes messages after they are sent to the returned channel
func NewBatchDeleter(ctx context.Context, wg *sync.WaitGroup, s *SQSService, every, drainTimeout time.Duration) chan<- *sqs.Message {
	dq := &deleteQueue{
		svc:                 s,
		accumulationTimeout: every,
		drainTimeout:        drainTimeout,
		queue:               make(chan *sqs.Message),
	}
	wg.Add(1)
	go dq.start(ctx, wg)
	return dq.queue
}

func (dq *deleteQueue) addToPendingDeletes(msg *sqs.Message) {
	dq.Lock()
	defer dq.Unlock()

	for _, e := range dq.entries {
		if *msg.MessageId == *e.Id {
			return
		}
	}

	dq.entries = append(dq.entries, &sqs.DeleteMessageBatchRequestEntry{
		Id:            msg.MessageId,
		ReceiptHandle: msg.ReceiptHandle,
	})
}

// deleteBatch deletes a batch of messages and returns the list of messages that failed to delete or an error for overall failure.
func (dq *deleteQueue) deleteBatch(msgs []*sqs.DeleteMessageBatchRequestEntry) ([]*sqs.DeleteMessageBatchRequestEntry, error) {
	req := &sqs.DeleteMessageBatchInput{
		QueueUrl: dq.svc.URL,
		Entries:  msgs,
	}

	resp, err := dq.svc.Svc.DeleteMessageBatch(req)
	if err != nil {
		dq.svc.Logger("Error deleting messages: %s", err)
		return nil, err
	}

	var failed []*sqs.DeleteMessageBatchRequestEntry
	for _, f := range resp.Failed {
		for _, m := range msgs {
			if *m.Id == *f.Id {
				failed = append(failed, m)
				break
			}
		}
	}
	return failed, nil
}

// deleteFromPending prepares a batch of messages and deletes them
func (dq *deleteQueue) deleteFromPending() {
	dq.Lock()
	defer dq.Unlock()

	n := len(dq.entries)
	if n > awsBatchSizeLimit {
		n = awsBatchSizeLimit
	}
	fails, err := dq.deleteBatch(dq.entries[:n])
	if err != nil {
		dq.svc.Logger("Error deleting batch: %s", err)
		return
	}

	dq.entries = dq.entries[n:]

	if len(fails) > 0 {
		dq.entries = append(dq.entries, fails...)
	}
}

func (dq *deleteQueue) start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// read from the delete queue accumulating batches and running delete every 10 items or 250ms
	for {
		select {
		case msg := <-dq.queue:
			dq.addToPendingDeletes(msg)
			dq.Lock()
			n := len(dq.entries)
			dq.Unlock()
			if n >= awsBatchSizeLimit {
				dq.deleteFromPending()
			}
		case <-time.After(dq.accumulationTimeout):
			dq.Lock()
			n := len(dq.entries)
			dq.Unlock()
			if n > 0 {
				dq.deleteFromPending()
			}
		}
		select {
		case <-ctx.Done():
			dq.drain()
			// drain the delete queue and return
			go func() {
				for {
					<-dq.queue
				}
			}()
			return
		default:
		}
	}
}

func (dq *deleteQueue) drain() {
	for {
		select {
		case msg := <-dq.queue:
			dq.addToPendingDeletes(msg)
			dq.deleteFromPending()
		case <-time.After(dq.drainTimeout):
			return
		}
	}
}
