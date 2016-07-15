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
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestQueueConsumerRunProcessesMessages(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	// delay so that the cancel occurs during 2nd receive
	delay := func(x interface{}) {
		time.Sleep(10 * time.Millisecond)
	}
	m := mock.NewMockSQSAPI(ctl)
	received := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			&sqs.Message{MessageId: aws.String("i1"), ReceiptHandle: aws.String("r1")},
			&sqs.Message{MessageId: aws.String("i2"), ReceiptHandle: aws.String("r2")},
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
}

func TestQueueConsumerRunDoesNotFetchMoreMessagesThanItCanProcess(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	m := mock.NewMockSQSAPI(ctl)
	received := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			&sqs.Message{MessageId: aws.String("i1"), ReceiptHandle: aws.String("r1")},
			&sqs.Message{MessageId: aws.String("i2"), ReceiptHandle: aws.String("r2")},
			&sqs.Message{MessageId: aws.String("i3"), ReceiptHandle: aws.String("r3")},
			&sqs.Message{MessageId: aws.String("i4"), ReceiptHandle: aws.String("r4")},
			&sqs.Message{MessageId: aws.String("i5"), ReceiptHandle: aws.String("r5")},
			&sqs.Message{MessageId: aws.String("i6"), ReceiptHandle: aws.String("r6")},
			&sqs.Message{MessageId: aws.String("i7"), ReceiptHandle: aws.String("r7")},
			&sqs.Message{MessageId: aws.String("i8"), ReceiptHandle: aws.String("r8")},
			&sqs.Message{MessageId: aws.String("i9"), ReceiptHandle: aws.String("r9")},
			&sqs.Message{MessageId: aws.String("i10"), ReceiptHandle: aws.String("r10")},
		},
	}

	// return 10 messages - the first 10 will never finish so the second batch will block and there will be no third request
	m.EXPECT().ReceiveMessage(gomock.Any()).Return(received, nil).Times(2)

	// hang until cancelled
	fn := func(ctx context.Context, msg string) error {
		<-ctx.Done()
		return nil
	}

	s := &SQSService{Svc: m}
	q := NewConsumer(s, fn)
	q.delayAfterReceiveError = time.Millisecond

	// wait long enough to ensure ReceiveMessage would have been invoked multiple times if it was too greedy
	ctx, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)

	// record number of goroutines before run to ensure no leaks
	ngo := runtime.NumGoroutine()

	// run the fetcher
	q.Run(ctx)

	// ensure no routines were leaked
	time.Sleep(time.Millisecond)
	if !assert.Equal(t, ngo, runtime.NumGoroutine(), "Should not leak goroutines") {
		panic(1)
	}
}

func TestQueueConsumerRunStopsGracefullyWhenCancelled(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()

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
}

func TestQueueConsumerRunRetriesOnErrors(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()

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
}

func noop(ctx context.Context, msg string) error {
	return nil
}
