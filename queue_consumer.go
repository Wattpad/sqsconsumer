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
		Logger:                 log.Printf, // or no-op?
	}
}

func (mf *Consumer) startWorkers(ctx context.Context, jobs <-chan *sqs.Message, wg *sync.WaitGroup) {
	for i := 0; i < receiveMessageBatchSize; i++ {
		go func() {
			for msg := range jobs {
				mID := aws.StringValue(msg.MessageId)

				msgCtx := sqsmessage.NewContext(ctx, msg)
				if err := mf.handler(msgCtx, aws.StringValue(msg.Body)); err != nil {
					mf.Logger("[%s] handler error: %s", mID, err)
				}
			}
			wg.Done()
		}()
	}
}

func (mf *Consumer) receiveMessages(ch chan<- *sqs.Message) {
	rcvParams := &sqs.ReceiveMessageInput{
		QueueUrl:            mf.s.URL,
		MaxNumberOfMessages: aws.Int64(receiveMessageBatchSize),
		WaitTimeSeconds:     aws.Int64(receiveMessageWaitSeconds),
		AttributeNames:      []*string{aws.String("SentTimestamp"), aws.String("ApproximateReceiveCount")},
	}

	for {
		resp, err := mf.s.Svc.ReceiveMessage(rcvParams)
		if err != nil {
			mf.Logger("Error receiving messages: %v", err)
			mf.Logger("Waiting before trying again")
			time.Sleep(mf.delayAfterReceiveError)
			continue
		}

		for _, msg := range resp.Messages {
			ch <- msg
		}
	}
}

// Run starts the Consumer, gracefully stopping it when the given context is cancelled.
func (mf *Consumer) Run(ctx context.Context) error {
	// start a batch-worth of handler-runners
	wg := &sync.WaitGroup{}
	wg.Add(receiveMessageBatchSize)
	jobs := make(chan *sqs.Message)
	mf.startWorkers(ctx, jobs, wg)

	messages := make(chan *sqs.Message)
	go mf.receiveMessages(messages)
	var done bool
	for {
		// Stop if the context was cancelled
		select {
		case <-ctx.Done():
			log.Println("Stopping worker")
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case msg := <-messages:
			if done {
				break
			}
			select {
			case <-ctx.Done():
				done = true
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
	Logger                 func(string, ...interface{})
}
