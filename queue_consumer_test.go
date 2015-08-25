package sqsconsumer

import (
	"testing"

	"time"

	"runtime"
	"sync/atomic"

	"io/ioutil"
	"log"
	"os"

	"github.com/Wattpad/sqsconsumer/mock"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestQueueConsumerRun(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	Convey("QueueConsumer.Run()", t, func() {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		Convey("Processes received messages", func() {
			// delay so that the cancel occurs during 2nd receive
			delay := func(x interface{}) {
				time.Sleep(10 * time.Millisecond)
			}
			m := mock.NewMockSQSAPI(ctl)
			received := &sqs.ReceiveMessageOutput{
				Messages: []*sqs.Message{
					&sqs.Message{MessageID: aws.String("i1"), ReceiptHandle: aws.String("r1")},
					&sqs.Message{MessageID: aws.String("i2"), ReceiptHandle: aws.String("r2")},
				},
			}

			// return 2 messages the first time, and an error the second time
			first := m.EXPECT().ReceiveMessage(gomock.Any()).Do(delay).Return(received, nil)
			m.EXPECT().ReceiveMessage(gomock.Any()).Do(delay).Return(nil, assert.AnError).After(first).AnyTimes()

			// count messages processed
			var callCount int64
			fn := func(ctx context.Context, msg string) error {
				atomic.AddInt64(&callCount, 1)
				return nil
			}

			s := &SQSService{Svc: m}
			q := NewConsumer(s, fn)
			q.delayAfterReceiveError = time.Millisecond

			// wait long enough to ensure ReceiveMessage is running
			ctx, _ := context.WithTimeout(context.Background(), 15*time.Millisecond)

			// record number of goroutines before run to ensure no leaks
			ngo := runtime.NumGoroutine()

			// run the fetcher
			q.Run(ctx)

			// ensure no routines were leaked
			time.Sleep(time.Millisecond)
			if !assert.Equal(t, ngo, runtime.NumGoroutine(), "Should not leak goroutines") {
				panic(1)
			}

			// ensure all messages were processed
			assert.Equal(t, int64(2), callCount)
		})

		Convey("Stops gracefully when cancelled", func() {
			// delay so that the cancel occurs mid-receive
			delay := func(x interface{}) {
				time.Sleep(10 * time.Millisecond)
			}
			m := mock.NewMockSQSAPI(ctl)
			m.EXPECT().ReceiveMessage(gomock.Any()).Do(delay).Return(&sqs.ReceiveMessageOutput{}, nil).AnyTimes()
			s := &SQSService{Svc: m}

			q := NewConsumer(s, noop)
			q.delayAfterReceiveError = time.Millisecond

			ngo := runtime.NumGoroutine()

			// wait long enough to ensure ReceiveMessage is running
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Millisecond)
			err := q.Run(ctx)

			assert.Error(t, err)

			time.Sleep(time.Millisecond) // time for goroutines to end
			assert.Equal(t, ngo, runtime.NumGoroutine(), "Should not leak goroutines")
		})

		Convey("Retries after an error from ReceiveMessage", func() {
			// delay so that the cancel occurs after 2 receives
			receiveCount := 0
			delay := func(x interface{}) {
				receiveCount++
				time.Sleep(2 * time.Millisecond)
			}
			m := mock.NewMockSQSAPI(ctl)
			m.EXPECT().ReceiveMessage(gomock.Any()).Do(delay).Return(nil, assert.AnError).AnyTimes()
			s := &SQSService{Svc: m}

			q := NewConsumer(s, noop)
			q.delayAfterReceiveError = time.Millisecond

			ngo := runtime.NumGoroutine()

			// wait long enough to ensure ReceiveMessage ran at least twice
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Millisecond)
			q.Run(ctx)

			assert.InDelta(t, 2, receiveCount, 1, "ReceiveMessage should have been retried 1-3 times")

			time.Sleep(time.Millisecond) // time for goroutines to end
			assert.Equal(t, ngo, runtime.NumGoroutine(), "Should not leak goroutines")
		})
	})
}

func TestSetupQueue(t *testing.T) {
	Convey("SetupQueue()", t, func() {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		name := "fake_queue_name"

		svc := mock.NewMockSQSAPI(ctl)

		Convey("Given SQS will return that the queue already exists", func() {
			svc.EXPECT().GetQueueURL(&sqs.GetQueueURLInput{QueueName: aws.String(name)}).Return(&sqs.GetQueueURLOutput{QueueURL: aws.String("http://example.com/queue/" + name)}, nil)

			Convey("When SetupQueue is invoked", func() {
				url, err := SetupQueue(svc, name)

				Convey("Then the result will be a URL that ends with the given queue name", func() {
					So(*url, ShouldEndWith, "/fake_queue_name")
				})

				Convey("And no error", func() {
					So(err, ShouldBeNil)
				})
			})
		})

		Convey("Given SQS creates the new queue successfully", func() {
			svc.EXPECT().GetQueueURL(gomock.Any()).Return(nil, assert.AnError)
			svc.EXPECT().CreateQueue(gomock.Any()).Return(&sqs.CreateQueueOutput{QueueURL: aws.String("http://example.com/queue/" + name)}, nil)

			Convey("When SetupQueue is invoked", func() {
				url, err := SetupQueue(svc, name)

				Convey("Then the result will be a URL that ends with the given queue name", func() {
					So(*url, ShouldEndWith, "/fake_queue_name")
				})

				Convey("And no error", func() {
					So(err, ShouldBeNil)
				})
			})
		})

		Convey("Given SQS returns an error", func() {
			svc.EXPECT().GetQueueURL(gomock.Any()).Return(nil, assert.AnError)
			svc.EXPECT().CreateQueue(gomock.Any()).Return(nil, assert.AnError)

			Convey("When SetupQueue is invoked", func() {
				url, err := SetupQueue(svc, name)

				Convey("Then the result will be a nil URL", func() {
					So(url, ShouldBeNil)
				})

				Convey("And a non-nil error", func() {
					So(err, ShouldNotBeNil)
				})
			})
		})
	})
}

func noop(ctx context.Context, msg string) error {
	return nil
}
