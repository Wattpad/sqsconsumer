package middleware

import (
	"io/ioutil"
	"log"
	"os"
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

func TestSQSVisibilityTimeoutExtender(t *testing.T) {
	// log to /dev/null because the extender is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	m := mock.NewMockSQSAPI(ctl)
	v := newDefaultVisibilityTimeoutExtender(&sqsconsumer.SQSService{Svc: m}, noop)

	assert.Equal(t, 25*time.Second, v.every)
	assert.Equal(t, int64(30), v.extensionSecs)
}

func TestSQSVisibilityTimeoutExtenderCustomDuration(t *testing.T) {
	// log to /dev/null because the extender is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	duration := 3 * time.Millisecond
	m := mock.NewMockSQSAPI(ctl)
	v := newDefaultVisibilityTimeoutExtender(&sqsconsumer.SQSService{Svc: m}, noop, OptEveryDuration(duration))

	assert.Equal(t, duration, v.every)
}

func TestSQSVisibilityTimeoutExtenderCustomExtension(t *testing.T) {
	// log to /dev/null because the extender is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	extension := int64(3)
	m := mock.NewMockSQSAPI(ctl)
	v := newDefaultVisibilityTimeoutExtender(&sqsconsumer.SQSService{Svc: m}, noop, OptExtensionSecs(extension))

	assert.Equal(t, extension, v.extensionSecs)
}

func TestSQSVisibilityTimeoutExtenderSuccess(t *testing.T) {
	// log to /dev/null because the extender is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	handler := testHandlerReturnAfterDelay(true, 15*time.Millisecond)

	msg := &sqs.Message{MessageId: aws.String("i1"), ReceiptHandle: aws.String("r1")}
	url := aws.String("an_url")

	m := mock.NewMockSQSAPI(ctl)
	m.EXPECT().ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: aws.Int64(1),
		QueueUrl:          url,
	}).Return(&sqs.ChangeMessageVisibilityOutput{}, nil)

	s := &sqsconsumer.SQSService{Svc: m, URL: url}

	ex := SQSVisibilityTimeoutExtender(s, OptEveryDuration(10*time.Millisecond), OptExtensionSecs(1))

	ctx := sqsmessage.NewContext(context.Background(), msg)
	ex(handler)(ctx, "")

	// pause briefly to ensure all the async stuff has resolved before evaluating expectations
	time.Sleep(time.Millisecond)

	ctl.Finish()
}

func TestSQSVisibilityTimeoutExtenderFailure(t *testing.T) {
	// log to /dev/null because the extender is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	// delay of 0ms
	handler := testHandlerReturnAfterDelay(false, 0*time.Millisecond)

	m := mock.NewMockSQSAPI(ctl)
	m.EXPECT().ChangeMessageVisibility(gomock.Any()).Times(0)
	s := &sqsconsumer.SQSService{Svc: m, URL: aws.String("an_url")}

	ex := SQSVisibilityTimeoutExtender(s, OptEveryDuration(5*time.Millisecond))

	msg := &sqs.Message{MessageId: aws.String("i1"), ReceiptHandle: aws.String("r1")}
	ctx := sqsmessage.NewContext(context.Background(), msg)

	ex(handler)(ctx, "a message")

	// extender runs after 5ms, so wait 10ms to ensure detecting change visibility requests that should not happen
	time.Sleep(10 * time.Millisecond)

	ctl.Finish()
}

func noop(ctx context.Context, msg string) error {
	return nil
}
