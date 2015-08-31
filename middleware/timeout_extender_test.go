package middleware

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/Wattpad/sqsconsumer"
	"github.com/Wattpad/sqsconsumer/mock"
	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSQSVisibilityTimeoutExtender(t *testing.T) {
	// log to /dev/null because the extender is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	Convey("SQSVisibilityTimeoutExtender", t, func() {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		Convey("When created with no options", func() {
			m := mock.NewMockSQSAPI(ctl)
			v := newDefaultVisibilityTimeoutExtender(&sqsconsumer.SQSService{Svc: m}, noop)

			Convey("Should default to run every 25 seconds", func() {
				So(v.every, ShouldEqual, 25*time.Second)
			})

			Convey("Should default to extend visibility timeout by 30 seconds", func() {
				So(v.extensionSecs, ShouldEqual, 30)
			})
		})

		Convey("When created with an OptEveryDuration", func() {
			duration := 3 * time.Millisecond
			m := mock.NewMockSQSAPI(ctl)
			v := newDefaultVisibilityTimeoutExtender(&sqsconsumer.SQSService{Svc: m}, noop, OptEveryDuration(duration))

			Convey("Should use the specified duration", func() {
				So(v.every, ShouldEqual, duration)
			})
		})

		Convey("When created with an OptExtensionSecs", func() {
			extension := int64(3)
			m := mock.NewMockSQSAPI(ctl)
			v := newDefaultVisibilityTimeoutExtender(&sqsconsumer.SQSService{Svc: m}, noop, OptExtensionSecs(extension))

			Convey("Should use the specified extension", func() {
				So(v.extensionSecs, ShouldEqual, extension)
			})
		})

		Convey("Given a MessageHandlerFunc that succeeds after 15ms", func() {
			handler := testHandlerReturnAfterDelay(true, 15*time.Millisecond)

			Convey("And the expectation that ChangeMessageVisibility will be called", func() {
				msg := &sqs.Message{MessageId: aws.String("i1"), ReceiptHandle: aws.String("r1")}
				url := aws.String("an_url")

				m := mock.NewMockSQSAPI(ctl)
				m.EXPECT().ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					ReceiptHandle:     msg.ReceiptHandle,
					VisibilityTimeout: aws.Int64(1),
					QueueUrl:          url,
				}).Return(&sqs.ChangeMessageVisibilityOutput{}, nil)

				s := &sqsconsumer.SQSService{Svc: m, URL: url}

				Convey("And the handler is wrapped in an SQSVisibilityTimeoutExtender configured to run after 10ms with an extension of 1s", func() {
					ex := SQSVisibilityTimeoutExtender(s, OptEveryDuration(10*time.Millisecond), OptExtensionSecs(1))

					Convey("When invoking the wrapped handler with a message and an SQSMessage-enriched context", func() {
						ctx := sqsmessage.NewContext(context.Background(), msg)
						ex(handler)(ctx, "")

						// pause briefly to ensure all the async stuff has resolved before evaluating expectations
						time.Sleep(time.Millisecond)

						Convey("Then the expectation that ChangeMessageVisibility was invoked is satisfied", func() {
							ctl.Finish()
						})
					})
				})
			})
		})

		Convey("Given a MessageHandlerFunc that fails", func() {
			// delay of 0ms
			handler := testHandlerReturnAfterDelay(false, 0*time.Millisecond)

			Convey("And an expectation that ChangeMessageVisibility is never called", func() {
				m := mock.NewMockSQSAPI(ctl)
				m.EXPECT().ChangeMessageVisibility(gomock.Any()).Times(0)
				s := &sqsconsumer.SQSService{Svc: m, URL: aws.String("an_url")}

				Convey("And the handler is wrapped in an SQSVisibilityTimeoutExtender configured to run after 5ms", func() {
					ex := SQSVisibilityTimeoutExtender(s, OptEveryDuration(5*time.Millisecond))

					Convey("When invoking the wrapped handler that will fail and waiting longer than the extender timeout", func() {
						msg := &sqs.Message{MessageId: aws.String("i1"), ReceiptHandle: aws.String("r1")}
						ctx := sqsmessage.NewContext(context.Background(), msg)

						ex(handler)(ctx, "a message")

						// extender runs after 5ms, so wait 10ms to ensure detecting change visibility requests that should not happen
						time.Sleep(10 * time.Millisecond)

						Convey("Then the expectation should be satisfied", func() {
							ctl.Finish()
						})
					})
				})
			})
		})
	})
}

func noop(ctx context.Context, msg string) error {
	return nil
}
