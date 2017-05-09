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

func TestQueueConsumerExtendsLongJobs(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	// delay so that the cancel occurs during 2nd receive
	delay := func(x interface{}) {
		time.Sleep(150 * time.Millisecond)
	}
	m := mock.NewMockSQSAPI(ctl)
	received := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			&sqs.Message{MessageId: aws.String("i1"), ReceiptHandle: aws.String("r1")},
			&sqs.Message{MessageId: aws.String("i2"), ReceiptHandle: aws.String("r2")},
		},
	}

	// return messages the first time, and an error the second time
	first := m.EXPECT().ReceiveMessage(gomock.Any()).Return(received, nil)
	m.EXPECT().ReceiveMessage(gomock.Any()).Do(delay).Return(nil, assert.AnError).After(first).AnyTimes()
	m.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil).AnyTimes()

	var extendCount int64
	m.EXPECT().ChangeMessageVisibilityBatch(gomock.Any()).
		Do(func(r *sqs.ChangeMessageVisibilityBatchInput) {
			atomic.AddInt64(&extendCount, 1)
		}).
		AnyTimes().
		Return(&sqs.ChangeMessageVisibilityBatchOutput{}, nil)

	fn := func(ctx context.Context, msg string) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	s := &SQSService{Svc: m, Logger: NoopLogger}
	q := NewConsumer(s, fn)
	q.DeleteMessageAccumulatorTimeout = 10 * time.Millisecond
	q.DeleteMessageDrainTimeout = 100 * time.Millisecond
	q.ExtendVisibilityTimeoutBySeconds = 2
	q.ExtendVisibilityTimeoutEvery = 10 * time.Millisecond
	q.delayAfterReceiveError = time.Second

	// wait long enough to ensure ReceiveMessage is running
	ctx, _ := context.WithTimeout(context.Background(), 25*time.Millisecond)

	// record number of goroutines before run to ensure no leaks
	ngo := runtime.NumGoroutine()

	// run the fetcher
	q.Run(ctx)

	// ensure no routines were leaked other than the receive messages goroutine (leaks on purpose)
	time.Sleep(150 * time.Millisecond)
	assert.InDelta(t, ngo, runtime.NumGoroutine(), 2, "Should not leak goroutines")

	// ensure visibility timeout was extended at least once
	assert.NotZero(t, atomic.LoadInt64(&extendCount))
}
