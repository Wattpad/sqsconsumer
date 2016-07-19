package middleware

import (
	"errors"
	"testing"
	"time"

	"github.com/Wattpad/sqsconsumer"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestApplyDecoratorsToHandler(t *testing.T) {
	prefixer := func(prefix string) MessageHandlerDecorator {
		return func(next sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc {
			return func(ctx context.Context, msg string) error {
				return next(ctx, prefix+msg)
			}
		}
	}

	mc := &testMessageCapturer{}

	wrapped := ApplyDecoratorsToHandler(mc.handlerFunc, prefixer("a"), prefixer("b"), prefixer("c"))

	wrapped(context.Background(), "0")
	assert.Equal(t, "cba0", mc.msg)
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
