package middleware

import (
	"log"
	"sync"
	"time"

	"github.com/Wattpad/sqsconsumer"
	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/net/context"
)

type deleteQueue struct {
	queue chan *sqs.Message
	sync.Mutex
	entries []*sqs.DeleteMessageBatchRequestEntry

	svc                 sqsconsumer.SQSAPI
	url                 *string
	accumulationTimeout time.Duration
}

// SQSBatchDeleteOnSuccessWithTimeout decorates a MessageHandler to delete the message if processing succeeded.
func SQSBatchDeleteOnSuccessWithTimeout(ctx context.Context, s *sqsconsumer.SQSService, after time.Duration) MessageHandlerDecorator {
	dq := &deleteQueue{
		svc:                 s.Svc,
		url:                 s.URL,
		accumulationTimeout: after,
		queue:               make(chan *sqs.Message),
	}

	return func(fn sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc {
		go dq.start(ctx)

		return func(msgCtx context.Context, msg string) error {
			// process the message
			err := fn(msgCtx, msg)

			if err != nil {
				// processing failure. do not delete so that it will be redelivered
				log.Printf("Error processing message '%s': %s.", msg, err)
			} else {
				sqsMsg, ok := sqsmessage.FromContext(msgCtx)
				if ok {
					dq.queue <- sqsMsg
				}
			}

			return err
		}
	}
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

// deleteBatch deletes up to 10 messages and returns the list of messages that failed to delete or an error for overall failure.
func (dq *deleteQueue) deleteBatch(msgs []*sqs.DeleteMessageBatchRequestEntry) ([]*sqs.DeleteMessageBatchRequestEntry, error) {
	req := &sqs.DeleteMessageBatchInput{
		QueueUrl: dq.url,
		Entries:  msgs,
	}

	log.Printf("Deleting %d messages", len(msgs))
	resp, err := dq.svc.DeleteMessageBatch(req)
	if err != nil {
		log.Println("Error deleting messages:", err)
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

// AWS limits batch sizes to 10 messages
const deleteBatchSizeLimit = 10

// deleteFromPending returns the number of messages deleted.
func (dq *deleteQueue) deleteFromPending() int {
	dq.Lock()
	defer dq.Unlock()

	n := len(dq.entries)
	if n > deleteBatchSizeLimit {
		n = deleteBatchSizeLimit
	}
	fails, err := dq.deleteBatch(dq.entries[:n])
	if err != nil {
		log.Printf("Error deleting batch: %s", err)
		return 0
	}

	dq.entries = dq.entries[n:]

	if len(fails) > 0 {
		n -= len(fails)
		for _, m := range fails {
			log.Printf("Failed to delete message %s", aws.StringValue(m.Id))
		}
	}
	return n
}

func (dq *deleteQueue) start(ctx context.Context) {
	// read from the delete queue accumulating batches and running delete every 10 items or 250ms
	for {
		select {
		case <-ctx.Done():
			// drain the delete queue and return
			for {
				select {
				case <-dq.queue:
				default:
					return
				}
			}
		case msg := <-dq.queue:
			dq.addToPendingDeletes(msg)
			dq.Lock()
			n := len(dq.entries)
			dq.Unlock()
			if n >= deleteBatchSizeLimit {
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
	}
}
