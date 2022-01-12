package sqsconsumer

import (
	"errors"
	"sync"
	"time"

	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/net/context"
)

var (
	ErrShutdownChannelClosed = errors.New("shutDown channel is already closed")
)

// NewConsumer creates a Consumer that uses the given SQSService to connect and invokes the handler for each message received.
func NewConsumer(s *SQSService, handler MessageHandlerFunc) *Consumer {
	return &Consumer{
		s:                                s,
		handler:                          handler,
		Logger:                           NoopLogger,
		JobWorkersCount:                  defaultJobWorkersCount,
		delayAfterReceiveError:           defaultDelayAfterReceiveError,
		WaitSeconds:                      defaultReceiveMessageWaitSeconds,
		ReceiveVisibilityTimoutSeconds:   defaultReceiveVisibilityTimeoutSeconds,
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
	for i := 0; i < mf.JobWorkersCount; i++ {
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

func (mf *Consumer) receiveMessages(ctx context.Context, wg *sync.WaitGroup, done <-chan struct{}, ch chan<- job, dq chan<- *sqs.Message) {
	defer close(ch)

	rcvParams := &sqs.ReceiveMessageInput{
		QueueUrl:            mf.s.URL,
		MaxNumberOfMessages: aws.Int64(defaultMessagesBatchSizeLimit),
		WaitTimeSeconds:     aws.Int64(mf.WaitSeconds),
		VisibilityTimeout:   aws.Int64(mf.ReceiveVisibilityTimoutSeconds),
		AttributeNames:      []*string{aws.String("SentTimestamp"), aws.String("ApproximateReceiveCount")},
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
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

// Run starts the Consumer, stopping it when the given context is cancelled.
// To shut down without canceling the Context, and allow in-flight messages to drain,
// use the WithShutdownChan RunOption.
//
// If the context is canceled, the returned error is the context's error.
// If the optional shutDown channel is closed before Run is called, the returned error is ErrShutdownChannelClosed.
// If in-flight messages drain to completion after shutdown, the returned error is nil.
func (mf *Consumer) Run(ctx context.Context, opts ...RunOption) error {
	ro := resolveRunOptions(opts)
	select {
	case <-ro.shutDown:
		return ErrShutdownChannelClosed
	default:
	}

	wg := &sync.WaitGroup{}
	jobs := make(chan job)
	mf.startWorkers(ctx, jobs, wg)

	cleanupWG := &sync.WaitGroup{}
	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	del := NewBatchDeleter(cleanupCtx, cleanupWG, mf.s, mf.DeleteMessageAccumulatorTimeout, mf.DeleteMessageDrainTimeout)

	messages := make(chan job)
	go mf.receiveMessages(cleanupCtx, cleanupWG, ro.shutDown, messages, del)
	defer func() {
		close(jobs)
		wg.Wait()
		cleanupCancel()
		cleanupWG.Wait()
	}()

	for {
		// Stop if the context was cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobs <- msg:
			}
		}
	}
}

func resolveRunOptions(opts []RunOption) *runOpts {
	opt := &runOpts{}
	for _, fn := range opts {
		fn(opt)
	}
	return opt
}

type job struct {
	msg       *sqs.Message
	completed chan<- result
}

type result struct {
	msg     *sqs.Message
	success bool
}

// AWS SQS defaults
const (
	defaultDelayAfterReceiveError          = 5 * time.Second
	defaultReceiveVisibilityTimeoutSeconds = 30
	defaultExtendVisibilityBySeconds       = 60
	defaultExtendVisibilityEvery           = 30 * time.Second
	defaultDeleteAccumulatorTimeout        = 250 * time.Millisecond
	defaultDeleteMessageDrainTimeout       = time.Second
	defaultReceiveMessageWaitSeconds       = 20
	defaultMessagesBatchSizeLimit          = 10
)

const (
	// default number of concurrent goroutines processing SQS messages by calling Consumer.handler
	defaultJobWorkersCount = 10
)

// Consumer is an SQS queue consumer
type Consumer struct {
	s                                *SQSService
	handler                          MessageHandlerFunc
	Logger                           func(string, ...interface{})
	delayAfterReceiveError           time.Duration
	JobWorkersCount                  int
	WaitSeconds                      int64
	ReceiveVisibilityTimoutSeconds   int64
	ExtendVisibilityTimeoutBySeconds int64
	ExtendVisibilityTimeoutEvery     time.Duration
	DeleteMessageAccumulatorTimeout  time.Duration
	DeleteMessageDrainTimeout        time.Duration
}
