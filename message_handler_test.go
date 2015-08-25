package sqsconsumer

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

func TestApplyDecoratorsToHandler(t *testing.T) {
	Convey("ApplyDecoratorsToHandler", t, func() {
		Convey("Given a handler that adds a prefix to the message", func() {
			prefixer := func(prefix string) MessageHandlerDecorator {
				return func(next MessageHandlerFunc) MessageHandlerFunc {
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
