package sqsconsumer

import (
	"log"
	"time"

	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/net/context"
)

// SQSVisibilityExtender decorates a MessageHandler to periodically extend the visibility timeout until the handler is done
func SQSVisibilityExtender(s *SQSService, opts ...VisibilityExtenderOption) MessageHandlerDecorator {
	return func(fn MessageHandlerFunc) MessageHandlerFunc {
		extender := newDefaultVisibilityExtender(s, fn, opts...)
		return extender.messageHandlerFunc
	}
}

// VisibilityExtenderOption is an option that can be applied to an SQSVisibilityExtender
type VisibilityExtenderOption func(*visibilityExtender)

// OptEveryDuration modifies an SQSVisibilityExtender by changing the frequency of updating the extension
func OptEveryDuration(d time.Duration) VisibilityExtenderOption {
	return func(ve *visibilityExtender) {
		ve.every = d
	}
}

// OptExtensionSecs modifies an SQSVisibilityExtender by changing the length of the requested extension
func OptExtensionSecs(s int64) VisibilityExtenderOption {
	return func(ve *visibilityExtender) {
		ve.extensionSecs = s
	}
}

type visibilityExtender struct {
	srv           *SQSService
	every         time.Duration
	extensionSecs int64
	next          MessageHandlerFunc
}

func newDefaultVisibilityExtender(s *SQSService, fn MessageHandlerFunc, opts ...VisibilityExtenderOption) *visibilityExtender {
	ve := &visibilityExtender{
		srv:           s,
		every:         25 * time.Second,
		extensionSecs: 30,
		next:          fn,
	}
	ve.applyOpts(opts...)
	return ve
}

func (ve *visibilityExtender) applyOpts(opts ...VisibilityExtenderOption) {
	for _, o := range opts {
		o(ve)
	}
}

func (ve *visibilityExtender) messageHandlerFunc(ctx context.Context, msg string) error {
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

func (ve *visibilityExtender) extendVisibilityTimeout(msg *sqs.Message) bool {
	log.Println("Extending visibility timeout for message", aws.StringValue(msg.MessageID))
	p := &sqs.ChangeMessageVisibilityInput{
		QueueURL:          ve.srv.URL,
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: aws.Int64(ve.extensionSecs),
	}
	_, err := ve.srv.Svc.ChangeMessageVisibility(p)
	if err != nil {
		log.Println("Failed to extend visibility for message", aws.StringValue(msg.MessageID))
		return false
	}

	return true
}
