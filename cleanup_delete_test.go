package sqsconsumer

import (
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wattpad/sqsconsumer/mock"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestQueueConsumerDeletesOnSuccess(t *testing.T) {
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
			&sqs.Message{MessageId: aws.String("i3"), ReceiptHandle: aws.String("r3")},
			&sqs.Message{MessageId: aws.String("i4"), ReceiptHandle: aws.String("r4")},
			&sqs.Message{MessageId: aws.String("i5"), ReceiptHandle: aws.String("r5")},
			&sqs.Message{MessageId: aws.String("i6"), ReceiptHandle: aws.String("r6")},
			&sqs.Message{MessageId: aws.String("i7"), ReceiptHandle: aws.String("r7")},
			&sqs.Message{MessageId: aws.String("i8"), ReceiptHandle: aws.String("r8")},
			&sqs.Message{MessageId: aws.String("i9"), ReceiptHandle: aws.String("r9")},
			&sqs.Message{MessageId: aws.String("i10"), ReceiptHandle: aws.String("r10")},
			&sqs.Message{MessageId: aws.String("i11"), ReceiptHandle: aws.String("r11")},
			&sqs.Message{MessageId: aws.String("i12"), ReceiptHandle: aws.String("r12")},
		},
	}

	// return messages the first time, and an error the second time
	first := m.EXPECT().ReceiveMessage(gomock.Any()).Do(delay).Return(received, nil)
	m.EXPECT().ReceiveMessage(gomock.Any()).Do(delay).Return(nil, assert.AnError).After(first).AnyTimes()

	var delReq []sqs.DeleteMessageBatchInput
	m.EXPECT().DeleteMessageBatch(gomock.Any()).
		Do(func(r *sqs.DeleteMessageBatchInput) {
			delReq = append(delReq, *r)
		}).
		Return(&sqs.DeleteMessageBatchOutput{}, nil).
		AnyTimes()

	// count messages processed
	var callCount int64
	fn := func(ctx context.Context, msg string) error {
		atomic.AddInt64(&callCount, 1)
		return nil
	}

	s := &SQSService{Svc: m}
	q := NewConsumer(s, fn)
	q.DeleteMessageAccumulatorTimeout = time.Millisecond
	q.DeleteMessageDrainTimeout = 100 * time.Millisecond
	q.delayAfterReceiveError = time.Second

	// wait long enough to ensure ReceiveMessage is running
	ctx, _ := context.WithTimeout(context.Background(), 15*time.Millisecond)

	// record number of goroutines before run to ensure no leaks
	ngo := runtime.NumGoroutine()

	// run the fetcher
	q.Run(ctx)

	// ensure no routines were leaked other than the receive messages goroutine (leaks on purpose)
	time.Sleep(50 * time.Millisecond)
	assert.InDelta(t, ngo, runtime.NumGoroutine(), 2, "Should not leak goroutines")

	// ensure all messages were processed
	assert.Equal(t, int64(12), callCount)

	// ensure all messages were deleted
	assert.Len(t, delReq, 2)
	assert.Equal(t, 12, len(delReq[0].Entries)+len(delReq[1].Entries))
}

func TestQueueConsumerDoesNotDeleteOnFailure(t *testing.T) {
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

	var delReq []sqs.DeleteMessageBatchInput
	m.EXPECT().DeleteMessageBatch(gomock.Any()).
		Do(func(r *sqs.DeleteMessageBatchInput) {
			delReq = append(delReq, *r)
		}).
		Return(&sqs.DeleteMessageBatchOutput{}, nil).
		AnyTimes()

	// count messages processed
	var callCount int64
	fn := func(ctx context.Context, msg string) error {
		atomic.AddInt64(&callCount, 1)
		return assert.AnError
	}

	s := &SQSService{Svc: m}
	q := NewConsumer(s, fn)
	q.DeleteMessageAccumulatorTimeout = time.Millisecond
	q.DeleteMessageDrainTimeout = 100 * time.Millisecond
	q.delayAfterReceiveError = time.Second

	// wait long enough to ensure ReceiveMessage is running
	ctx, _ := context.WithTimeout(context.Background(), 15*time.Millisecond)

	// record number of goroutines before run to ensure no leaks
	ngo := runtime.NumGoroutine()

	// run the fetcher
	q.Run(ctx)

	// ensure no routines were leaked other than the receive messages goroutine (leaks on purpose)
	time.Sleep(50 * time.Millisecond)
	assert.InDelta(t, ngo, runtime.NumGoroutine(), 2, "Should not leak goroutines")

	// ensure all messages were processed
	assert.Equal(t, int64(2), callCount)

	// ensure no messages were deleted
	assert.Len(t, delReq, 0)
}
