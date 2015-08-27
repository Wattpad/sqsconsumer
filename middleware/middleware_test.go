package middleware

import (
	"errors"
	"testing"
	"time"

	"github.com/Wattpad/sqsconsumer"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

func TestApplyDecoratorsToHandler(t *testing.T) {
	Convey("ApplyDecoratorsToHandler", t, func() {
		Convey("Given a handler that adds a prefix to the message", func() {
			prefixer := func(prefix string) MessageHandlerDecorator {
				return func(next sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc {
					return func(ctx context.Context, msg string) error {
						return next(ctx, prefix+msg)
					}
				}
			}

			Convey("And a handler that allows introspecting its message argument", func() {
				mc := &testMessageCapturer{}

				Convey("When invoked with three prefixers 'a', 'b', 'c'", func() {
					wrapped := ApplyDecoratorsToHandler(mc.handlerFunc, prefixer("a"), prefixer("b"), prefixer("c"))

					Convey("And the wrapped handler is used on a message '0'", func() {
						wrapped(context.Background(), "0")

						Convey("Then the decorators should be applied in inverse order so the message received by the original handler should be 'cba0'", func() {
							So(mc.msg, ShouldEqual, "cba0")
						})
					})
				})
			})
		})
	})
}

// testMessageCapturer has a handler func that captures the message it received for examination
type testMessageCapturer struct {
	msg string
}

// handlerFunc is the MessageHandlerFunc for the testMessageCapturer
func (m *testMessageCapturer) handlerFunc(_ context.Context, msg string) error {
	m.msg = msg
	return nil
}

// testHandlerReturnAfterDelay generates a handler func that will succeed or error as directed after a delay
func testHandlerReturnAfterDelay(succeed bool, delay time.Duration) sqsconsumer.MessageHandlerFunc {
	return func(ctx context.Context, _ string) error {
		time.Sleep(delay)
		if !succeed {
			return errors.New("an error")
		}
		return nil
	}
}
