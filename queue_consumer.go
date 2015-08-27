package sqsconsumer

import (
	"log"
	"sync"
	"time"

	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/net/context"
)

// NewConsumer creates a Consumer that uses the given SQSService to connect and invokes the handler for each message received.
func NewConsumer(s *SQSService, handler MessageHandlerFunc) *Consumer {
	return &Consumer{
		s:                      s,
		handler:                handler,
		delayAfterReceiveError: defaultDelayAfterReceiveError,
	}
}

// Run starts the Consumer, gracefully stopping it when the given context is cancelled.
func (mf *Consumer) Run(ctx context.Context) error {
	// start a batch-worth of handler-runners
	wg := &sync.WaitGroup{}
	wg.Add(receiveMessageBatchSize)
	jobs := make(chan *sqs.Message)
	for i := 0; i < receiveMessageBatchSize; i++ {
		go func() {
			for msg := range jobs {
				mID := aws.StringValue(msg.MessageID)
				log.Printf("Processing message %s", mID)

				msgCtx := sqsmessage.NewContext(ctx, msg)
				if err := mf.handler(msgCtx, aws.StringValue(msg.Body)); err != nil {
					log.Printf("[%s] handler error: %s", mID, err)
				}
			}
			wg.Done()
		}()
	}

	rcvParams := &sqs.ReceiveMessageInput{
		QueueURL:            mf.s.URL,
		MaxNumberOfMessages: aws.Int64(receiveMessageBatchSize),
		WaitTimeSeconds:     aws.Int64(receiveMessageWaitSeconds),
	}
	for {
		// Stop if the context was cancelled
		select {
		case <-ctx.Done():
			log.Println("Stopping worker")
			close(jobs)
			wg.Wait()
			return ctx.Err()
		default:
		}

		resp, err := mf.s.Svc.ReceiveMessage(rcvParams)
		if err != nil {
			log.Println("Error receiving messages:", err)
			log.Println("Waiting before trying again")
			time.Sleep(mf.delayAfterReceiveError)
			continue
		}

		log.Printf("Received %d messages", len(resp.Messages))
	HANDLE_LOOP:
		for _, msg := range resp.Messages {
			select {
			// abort early if cancelled
			case <-ctx.Done():
				break HANDLE_LOOP
			case jobs <- msg:
			}
		}
	}
}

const (
	defaultDelayAfterReceiveError = 5 * time.Second

	// AWS maximums
	receiveMessageBatchSize   = 10
	receiveMessageWaitSeconds = 20
)

// Consumer is an SQS queue consumer
type Consumer struct {
	s                      *SQSService
	handler                MessageHandlerFunc
	delayAfterReceiveError time.Duration
}
