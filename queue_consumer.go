package sqsconsumer

import (
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
		s:                              s,
		handler:                        handler,
		delayAfterReceiveError:         defaultDelayAfterReceiveError,
		WaitSeconds:                    defaultReceiveMessageWaitSeconds,
		ReceiveVisibilityTimoutSeconds: defaultReceiveVisibilityTimeoutSeconds,
		Logger: NoopLogger,

		ExtendVisibilityTimeoutBySeconds: defaultExtendVisibilityBySeconds,
		ExtendVisibilityTimeoutEvery:     defaultExtendVisibilityEvery,
		DeleteMessageAccumulatorTimeout:  defaultDeleteAccumulatorTimeout,
		DeleteMessageDrainTimeout:        defaultDeleteMessageDrainTimeout,
	}
}

// SetLogger sets the consumer and service loggers to a function similar to fmt.Printf
func (mf *Consumer) SetLogger(fn func(format string, args ...interface{})) {
	mf.Logger = fn
	mf.s.Logger = fn
}

func (mf *Consumer) startWorkers(ctx context.Context, jobs <-chan job, wg *sync.WaitGroup) {
	for i := 0; i < awsBatchSizeLimit; i++ {
		wg.Add(1)
		go func() {
			for j := range jobs {
				msgCtx := sqsmessage.NewContext(ctx, j.msg)
				err := mf.handler(msgCtx, aws.StringValue(j.msg.Body))
				if err != nil {
					mf.Logger("[%s] handler error: %s", aws.StringValue(j.msg.MessageId), err)
				}

				j.completed <- result{
					msg:     j.msg,
					success: err == nil,
				}
			}
			wg.Done()
		}()
	}
}

func (mf *Consumer) startBatchExtender(ctx context.Context, wg *sync.WaitGroup, del chan<- *sqs.Message, pending []*sqs.Message) chan<- result {
	results := make(chan result, len(pending))

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(mf.ExtendVisibilityTimeoutEvery)
		ext := NewBatchVisibilityExtender(ctx, mf.s, ticker.C, mf.ExtendVisibilityTimeoutBySeconds, pending)

		left := len(pending)
		for left > 0 {
			select {
			case r := <-results:
				if r.success {
					del <- r.msg
				}
				ext <- r.msg
				left--
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

	return results
}

func (mf *Consumer) receiveMessages(ctx context.Context, wg *sync.WaitGroup, ch chan<- job, dq chan<- *sqs.Message) {
	rcvParams := &sqs.ReceiveMessageInput{
		QueueUrl:            mf.s.URL,
		MaxNumberOfMessages: aws.Int64(awsBatchSizeLimit),
		WaitTimeSeconds:     aws.Int64(mf.WaitSeconds),
		VisibilityTimeout:   aws.Int64(mf.ReceiveVisibilityTimoutSeconds),
		AttributeNames:      []*string{aws.String("SentTimestamp"), aws.String("ApproximateReceiveCount")},
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		resp, err := mf.s.Svc.ReceiveMessage(rcvParams)
		if err != nil {
			mf.Logger("Error receiving messages: %v", err)
			mf.Logger("Waiting before trying again")
			time.Sleep(mf.delayAfterReceiveError)
			continue
		}

		if len(resp.Messages) == 0 {
			continue
		}

		completed := mf.startBatchExtender(ctx, wg, dq, resp.Messages)

		for _, msg := range resp.Messages {
			ch <- job{
				msg:       msg,
				completed: completed,
			}
		}
	}
}

// Run starts the Consumer, gracefully stopping it when the given context is cancelled.
func (mf *Consumer) Run(ctx context.Context) error {
	wg := &sync.WaitGroup{}
	jobs := make(chan job)
	mf.startWorkers(ctx, jobs, wg)

	cleanupWG := &sync.WaitGroup{}
	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	del := NewBatchDeleter(cleanupCtx, cleanupWG, mf.s, mf.DeleteMessageAccumulatorTimeout, mf.DeleteMessageDrainTimeout)

	messages := make(chan job)
	go mf.receiveMessages(cleanupCtx, cleanupWG, messages, del)
	var done bool
	for {
		// Stop if the context was cancelled
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			cleanupCancel()
			cleanupWG.Wait()
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

type job struct {
	msg       *sqs.Message
	completed chan<- result
}

type result struct {
	msg     *sqs.Message
	success bool
}

const (
	defaultDelayAfterReceiveError          = 5 * time.Second
	defaultReceiveVisibilityTimeoutSeconds = 30
	defaultExtendVisibilityBySeconds       = 60
	defaultExtendVisibilityEvery           = 30 * time.Second
	defaultDeleteAccumulatorTimeout        = 250 * time.Millisecond
	defaultDeleteMessageDrainTimeout       = time.Second

	// AWS maximums
	awsBatchSizeLimit                = 10
	defaultReceiveMessageWaitSeconds = 20
)

// Consumer is an SQS queue consumer
type Consumer struct {
	s                              *SQSService
	handler                        MessageHandlerFunc
	delayAfterReceiveError         time.Duration
	Logger                         func(string, ...interface{})
	WaitSeconds                    int64
	ReceiveVisibilityTimoutSeconds int64

	ExtendVisibilityTimeoutBySeconds int64
	ExtendVisibilityTimeoutEvery     time.Duration
	DeleteMessageAccumulatorTimeout  time.Duration
	DeleteMessageDrainTimeout        time.Duration
}