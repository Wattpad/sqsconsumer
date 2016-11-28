package sqsconsumer

import (
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/net/context"
)

type visibilityExtenderQueue struct {
	queue chan *sqs.Message
	sync.Mutex
	entries []*sqs.Message

	svc           SQSAPI
	url           *string
	extensionSecs int64
	ticker        <-chan time.Time
}

// NewBatchBatchVisibilityExtender starts a batch visibility extender routine that extends visibilty on messages until they are sent to the returned channel
func NewBatchVisibilityExtender(ctx context.Context, s *SQSService, ticker <-chan time.Time, extensionSecs int64, pending []*sqs.Message) chan<- *sqs.Message {
	entries := make([]*sqs.Message, len(pending))
	copy(entries, pending)

	veq := &visibilityExtenderQueue{
		svc:           s.Svc,
		url:           s.URL,
		ticker:        ticker,
		extensionSecs: extensionSecs,
		queue:         make(chan *sqs.Message, len(pending)),
		entries:       entries,
	}
	go veq.start(ctx)
	return veq.queue
}

func (veq *visibilityExtenderQueue) removeFromPending(msg *sqs.Message) {
	veq.Lock()
	defer veq.Unlock()

	for i, e := range veq.entries {
		if *msg.MessageId == *e.MessageId {
			veq.entries = append(veq.entries[:i], veq.entries[i+1:]...)
			return
		}
	}
}

// extendBatch extends the visibility of up to 10 messages and returns the list of messages that failed to extend and an error for overall failure.
func (veq *visibilityExtenderQueue) extendBatch(msgs []*sqs.Message) ([]*sqs.Message, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	var entries []*sqs.ChangeMessageVisibilityBatchRequestEntry
	for _, m := range msgs {
		log.Println("Extending visibility timeout for message", aws.StringValue(m.MessageId))
		entries = append(entries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                m.MessageId,
			ReceiptHandle:     m.ReceiptHandle,
			VisibilityTimeout: aws.Int64(veq.extensionSecs),
		})
	}

	p := &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: veq.url,
		Entries:  entries,
	}
	resp, err := veq.svc.ChangeMessageVisibilityBatch(p)
	if err != nil {
		log.Println("Error extending messages:", err)
		return msgs, err
	}

	var failed []*sqs.Message
	for _, f := range resp.Failed {
		for _, m := range msgs {
			if *m.MessageId == *f.Id {
				failed = append(failed, m)
				break
			}
		}
	}
	return failed, nil
}

// extendPending extends all the pending messages
func (veq *visibilityExtenderQueue) extendPending() {
	veq.Lock()
	defer veq.Unlock()

	fails, err := veq.extendBatch(veq.entries)
	if err != nil {
		log.Printf("Error extending batch: %s", err)
	}

	var retries uint
	for len(fails) > 0 && retries <= 5 {
		retries++
		time.Sleep(time.Duration((2<<retries)*50) * time.Millisecond)
		fails, err = veq.extendBatch(fails)
		if err != nil {
			log.Printf("Error extending batch: %s", err)
		}
	}
}

func (veq *visibilityExtenderQueue) start(ctx context.Context) {
	// read from the queue to remove pending items, and extending visibility for all still spending messages periodically
	for {
		select {
		case <-ctx.Done():
			// drain the queue and return
			for {
				select {
				case <-veq.queue:
				default:
					return
				}
			}
		case msg := <-veq.queue:
			veq.removeFromPending(msg)
		case <-veq.ticker:
			veq.extendPending()
		}

		veq.Lock()
		n := len(veq.entries)
		veq.Unlock()
		if n <= 0 {
			return
		}
	}
}
