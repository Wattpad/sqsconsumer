package middleware

import (
	"log"
	"time"

	"github.com/Wattpad/sqsconsumer"
	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/net/context"
)

// SQSVisibilityTimeoutExtender decorates a MessageHandler to periodically extend the visibility timeout until the handler is done
func SQSVisibilityTimeoutExtender(s *sqsconsumer.SQSService, opts ...VisibilityTimeoutExtenderOption) MessageHandlerDecorator {
	return func(fn sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc {
		extender := newDefaultVisibilityTimeoutExtender(s, fn, opts...)
		return extender.messageHandlerFunc
	}
}

// VisibilityTimeoutExtenderOption is an option that can be applied to an SQSVisibilityTimeoutExtender
type VisibilityTimeoutExtenderOption func(*visibilityTimeoutExtender)

// OptEveryDuration modifies an SQSVisibilityTimeoutExtender by changing the frequency of updating the extension
func OptEveryDuration(d time.Duration) VisibilityTimeoutExtenderOption {
	return func(ve *visibilityTimeoutExtender) {
		ve.every = d
	}
}

// OptExtensionSecs modifies an SQSVisibilityTimeoutExtender by changing the length of the requested extension
func OptExtensionSecs(s int64) VisibilityTimeoutExtenderOption {
	return func(ve *visibilityTimeoutExtender) {
		ve.extensionSecs = s
	}
}

type visibilityTimeoutExtender struct {
	srv           *sqsconsumer.SQSService
	every         time.Duration
	extensionSecs int64
	next          sqsconsumer.MessageHandlerFunc
}

const (
	defaultVisibilityTimeoutExtenderFrequency = 25 * time.Second
	defaultVisibilityTimeoutExtensionSeconds  = 30
)

func newDefaultVisibilityTimeoutExtender(s *sqsconsumer.SQSService, fn sqsconsumer.MessageHandlerFunc, opts ...VisibilityTimeoutExtenderOption) *visibilityTimeoutExtender {
	ve := &visibilityTimeoutExtender{
		srv:           s,
		every:         defaultVisibilityTimeoutExtenderFrequency,
		extensionSecs: defaultVisibilityTimeoutExtensionSeconds,
		next:          fn,
	}
	ve.applyOpts(opts...)
	return ve
}

func (ve *visibilityTimeoutExtender) applyOpts(opts ...VisibilityTimeoutExtenderOption) {
	for _, o := range opts {
		o(ve)
	}
}

func (ve *visibilityTimeoutExtender) messageHandlerFunc(ctx context.Context, msg string) error {
	ticker := time.NewTicker(ve.every)
	done := make(chan struct{})
	go func() {
		sqsMsg, ok := sqsmessage.FromContext(ctx)
		if !ok {
			return
		}

		for {
			select {
			case <-ticker.C:
				ve.extendVisibilityTimeout(sqsMsg)
			case <-done:
				return
			}
		}
	}()

	// process the message
	err := ve.next(ctx, msg)

	// stop extending the visibility timeout
	ticker.Stop()
	close(done)

	return err
}

func (ve *visibilityTimeoutExtender) extendVisibilityTimeout(msg *sqs.Message) bool {
	log.Println("Extending visibility timeout for message", aws.StringValue(msg.MessageId))
	p := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          ve.srv.URL,
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: aws.Int64(ve.extensionSecs),
	}
	_, err := ve.srv.Svc.ChangeMessageVisibility(p)
	if err != nil {
		log.Println("Failed to extend visibility timeout for message", aws.StringValue(msg.MessageId))
		return false
	}

	return true
}
