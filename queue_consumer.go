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

// SQSServiceForRegionAndQueue creates an AWS SQS client configured for the given region and gets or creates a queue with the given name
func SQSServiceForRegionAndQueue(region, queueName string) (*SQSService, error) {
	svc := sqs.New(&aws.Config{
		Region: aws.String(region),
	})
	s := &SQSService{Svc: svc}

	var url *string
	var err error
	if url, err = SetupQueue(svc, queueName); err != nil {
		return nil, err
	}
	s.URL = url

	return s, nil
}

// DefaultMiddlewareStack returns a reasonably configured middleware stack.
// It requires a context which, when cancelled, will clean up the delete queue and the max number of concurrent handlers to run at a time.
func DefaultMiddlewareStack(ctx context.Context, s *SQSService, numHandlers int) []MessageHandlerDecorator {
	extend := SQSVisibilityExtender(s)
	delete := SQSBatchDeleteOnSuccessWithTimeout(ctx, s, 250*time.Millisecond)
	limit := WorkerPoolMiddleware(numHandlers)
	return []MessageHandlerDecorator{extend, delete, limit}
}

// SetupQueue creates the queue to listen on and returns the URL
func SetupQueue(svc SQSAPI, name string) (*string, error) {
	// if the queue already exists just get the url
	getResp, err := svc.GetQueueURL(&sqs.GetQueueURLInput{
		QueueName: aws.String(name),
	})
	if err == nil {
		return getResp.QueueURL, nil
	}

	// fallback to creating the queue
	createResp, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]*string{
			"MessageRetentionPeriod":        aws.String("1209600"), // 14 days
			"ReceiveMessageWaitTimeSeconds": aws.String("20"),
		},
	})
	if err != nil {
		return nil, err
	}

	return createResp.QueueURL, nil
}

// Consumer is an SQS queue consumer
type Consumer struct {
	s                      *SQSService
	handler                MessageHandlerFunc
	delayAfterReceiveError time.Duration
}

// NewConsumer creates a Consumer that uses the given SQSService to connect and invokes the handler for each message received.
func NewConsumer(s *SQSService, handler MessageHandlerFunc) *Consumer {
	return &Consumer{
		s:                      s,
		handler:                handler,
		delayAfterReceiveError: 5 * time.Second,
	}
}

// Run starts the Consumer, gracefully stopping it when the given context is cancelled.
func (mf *Consumer) Run(ctx context.Context) error {
	wg := &sync.WaitGroup{}

	rcvParams := &sqs.ReceiveMessageInput{
		QueueURL:            mf.s.URL,
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(20),
	}
	for {
		// Stop if the context was cancelled
		select {
		case <-ctx.Done():
			log.Println("Stopping worker")
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
		for i, msg := range resp.Messages {
			select {
			// abort early if cancelled
			case <-ctx.Done():
				break HANDLE_LOOP

			default:
				wg.Add(1)
				msgCtx := sqsmessage.NewContext(ctx, msg)
				msgCtx, _ = context.WithCancel(ctx)

				// run the handler in a goroutine
				go func(ctx context.Context, msg *sqs.Message) {
					log.Printf("Processing message %d", i)
					if err := mf.handler(ctx, aws.StringValue(msg.Body)); err != nil {
						log.Printf("[%s] handler error: %s", aws.StringValue(msg.MessageID), err)
					}
					wg.Done()
				}(msgCtx, msg)
			}
		}
	}
}
