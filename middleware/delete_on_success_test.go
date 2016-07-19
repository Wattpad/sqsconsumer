package middleware

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/Wattpad/sqsconsumer"
	"github.com/Wattpad/sqsconsumer/mock"
	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestDeleteQueueDeleteBatchSuccess(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mSQS := mock.NewMockSQSAPI(ctl)
	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

	q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)

	_, err := q.deleteBatch([]*sqs.DeleteMessageBatchRequestEntry{
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r1"), Id: aws.String("i1")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r2"), Id: aws.String("i2")},
	})

	assert.Nil(t, err)
}

func TestDeleteQueueDeleteBatchFailure(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)
	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{
			&sqs.BatchResultErrorEntry{Id: aws.String("i1")},
		},
	}, nil)

	q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)

	batch := []*sqs.DeleteMessageBatchRequestEntry{
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r1"), Id: aws.String("i1")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r2"), Id: aws.String("i2")},
	}
	fails, err := q.deleteBatch(batch)

	assert.Nil(t, err, "Err is nil because there was a valid response")

	assert.Equal(t, 1, len(fails), "1 failure")
	assert.Equal(t, batch[0], fails[0])
}

func TestDeleteQueueDeleteBatchErrors(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)
	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(nil, assert.AnError)

	q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)

	_, err := q.deleteBatch([]*sqs.DeleteMessageBatchRequestEntry{
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r1"), Id: aws.String("i1")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r2"), Id: aws.String("i2")},
	})

	assert.NotNil(t, err)
}

func TestDeleteQueueAddToPendingDeletes(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mSQS := mock.NewMockSQSAPI(ctl)
	q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)

	q.addToPendingDeletes(&sqs.Message{MessageId: aws.String("1")})
	q.addToPendingDeletes(&sqs.Message{MessageId: aws.String("2")})

	q.Lock()
	defer q.Unlock()
	assert.Equal(t, 2, len(q.entries))
}

func TestDeleteQueueAddToPendingDeletesIgnoreDupes(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)
	q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)

	q.addToPendingDeletes(&sqs.Message{MessageId: aws.String("1")})
	q.addToPendingDeletes(&sqs.Message{MessageId: aws.String("1")})

	q.Lock()
	defer q.Unlock()
	assert.Equal(t, 1, len(q.entries))
}

func TestDeleteQueueDeleteFromPendingSingle(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mSQS := mock.NewMockSQSAPI(ctl)
	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

	q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)
	q.entries = []*sqs.DeleteMessageBatchRequestEntry{
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("1")},
	}

	n := q.deleteFromPending()

	q.Lock()
	defer q.Unlock()
	assert.Equal(t, 0, len(q.entries))
	assert.Equal(t, 1, n)
}

func TestDeleteQueueDeleteFromPendingExecutesBatchesOf10(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)
	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

	q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)
	q.entries = []*sqs.DeleteMessageBatchRequestEntry{
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("1")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("2")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("3")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("4")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("5")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("6")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("7")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("8")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("9")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("10")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("11")},
	}

	n := q.deleteFromPending()

	q.Lock()
	defer q.Unlock()
	assert.Equal(t, 1, len(q.entries))
	assert.Equal(t, 10, n)
}

func TestDeleteQueueDeleteFromPending1Failure(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)
	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{
			&sqs.BatchResultErrorEntry{Id: aws.String("i1")},
		},
	}, nil)

	q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)
	q.entries = []*sqs.DeleteMessageBatchRequestEntry{
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r1"), Id: aws.String("i1")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r2"), Id: aws.String("i2")},
	}

	n := q.deleteFromPending()

	assert.Equal(t, 0, len(q.entries))
	assert.Equal(t, 1, n)
}

func TestDeleteQueueDeleteFromPendingSQSError(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)
	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(nil, errors.New("an error"))

	q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)
	q.entries = []*sqs.DeleteMessageBatchRequestEntry{
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r1"), Id: aws.String("i1")},
		&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r2"), Id: aws.String("i2")},
	}

	n := q.deleteFromPending()

	q.Lock()
	defer q.Unlock()
	assert.Equal(t, 2, len(q.entries))
	assert.Equal(t, 0, n)
}

func TestDeleteQueueStartAccumulatesFromAChannel(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mSQS := mock.NewMockSQSAPI(ctl)
	q := testMakeDeleteQueueWithTimeout(mSQS, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.start(ctx)

	q.queue <- &sqs.Message{MessageId: aws.String("1")}

	q.Lock()
	defer q.Unlock()
	assert.Len(t, q.entries, 1)
}

func TestDeleteQueueStartDoesNotLeakRoutines(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)

	// pause for Convey's goroutine mess to stabilize
	time.Sleep(time.Millisecond)

	// note the number of goroutines before starting
	n := runtime.NumGoroutine()

	// create the queue and run it
	q := testMakeDeleteQueueWithTimeout(mSQS, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go q.start(ctx)

	// cancel the queue context and pause to let the number of routines to stabilize
	cancel()
	time.Sleep(time.Millisecond)

	// verify no routines leaked
	assert.Equal(t, n, runtime.NumGoroutine())
}

func TestDeleteQueueStartDeletesBatchesAfterTimeout(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)
	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

	q := testMakeDeleteQueueWithTimeout(mSQS, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.start(ctx)

	q.queue <- &sqs.Message{MessageId: aws.String("1")}

	time.Sleep(15 * time.Millisecond)

	ctl.Finish()
}

func TestDeleteQueueStartDeletesBatchesOf10(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)
	q := testMakeDeleteQueueWithTimeout(mSQS, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.start(ctx)

	q.queue <- &sqs.Message{MessageId: aws.String("1")}
	q.queue <- &sqs.Message{MessageId: aws.String("2")}
	q.queue <- &sqs.Message{MessageId: aws.String("3")}
	q.queue <- &sqs.Message{MessageId: aws.String("4")}
	q.queue <- &sqs.Message{MessageId: aws.String("5")}
	q.queue <- &sqs.Message{MessageId: aws.String("6")}
	q.queue <- &sqs.Message{MessageId: aws.String("7")}
	q.queue <- &sqs.Message{MessageId: aws.String("8")}
	q.queue <- &sqs.Message{MessageId: aws.String("9")}

	time.Sleep(2 * time.Millisecond)

	// finish verifies the expectation, which is empty
	ctl.Finish()

	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

	q.queue <- &sqs.Message{MessageId: aws.String("10")}

	time.Sleep(2 * time.Millisecond)

	ctl.Finish()
}

func TestSQSBatchDeleteOnSuccessWithTimeout(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mSQS := mock.NewMockSQSAPI(ctl)
	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u := "a/url"
	srv := &sqsconsumer.SQSService{Svc: mSQS, URL: &u}

	deleter := SQSBatchDeleteOnSuccessWithTimeout(ctx, srv, 10*time.Millisecond)

	handler := func(_ context.Context, _ string) error { return nil }
	handler = deleter(handler)

	msg := &sqs.Message{MessageId: aws.String("1"), Body: aws.String("message")}
	msgCtx := sqsmessage.NewContext(ctx, msg)
	handler(msgCtx, *msg.Body)

	time.Sleep(15 * time.Millisecond)

	ctl.Finish()
}

func TestSQSBatchDeleteOnSuccessWithTimeoutAfter10Messages(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u := "a/url"
	srv := &sqsconsumer.SQSService{Svc: mSQS, URL: &u}

	deleter := SQSBatchDeleteOnSuccessWithTimeout(ctx, srv, 10*time.Millisecond)

	handler := func(_ context.Context, _ string) error { return nil }
	handler = deleter(handler)

	for i := 1; i <= 9; i++ {
		msg := &sqs.Message{
			MessageId: aws.String(fmt.Sprintf("%d", i)),
			Body:      aws.String("message"),
		}
		msgCtx := sqsmessage.NewContext(ctx, msg)
		handler(msgCtx, *msg.Body)
	}

	time.Sleep(2 * time.Millisecond)

	// finish verifies the expectation, which is empty
	ctl.Finish()

	mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

	msg := &sqs.Message{MessageId: aws.String("10"), Body: aws.String("message")}
	msgCtx := sqsmessage.NewContext(ctx, msg)
	handler(msgCtx, *msg.Body)

	time.Sleep(2 * time.Millisecond)

	ctl.Finish()
}

func TestSQSBatchDeleteOnSuccessWithTimeoutDoesNotDeleteOnError(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mSQS := mock.NewMockSQSAPI(ctl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u := "a/url"
	srv := &sqsconsumer.SQSService{Svc: mSQS, URL: &u}

	deleter := SQSBatchDeleteOnSuccessWithTimeout(ctx, srv, 10*time.Millisecond)

	handler := func(_ context.Context, _ string) error { return errors.New("an error") }
	handler = deleter(handler)

	msg := &sqs.Message{MessageId: aws.String("1"), Body: aws.String("message")}
	msgCtx := sqsmessage.NewContext(ctx, msg)
	handler(msgCtx, *msg.Body)

	time.Sleep(15 * time.Millisecond)

	ctl.Finish()
}

func testMakeDeleteQueueWithTimeout(s sqsconsumer.SQSAPI, timeout time.Duration) *deleteQueue {
	u := "url"
	return &deleteQueue{
		svc:                 s,
		url:                 &u,
		accumulationTimeout: timeout,
		queue:               make(chan *sqs.Message),
	}
}
