package sqsconsumer

import (
	"errors"
	"time"

	"golang.org/x/net/context"
)

// testHandlerReturnAfterDelay generates a handler func that will succeed or error as directed after a delay
func testHandlerReturnAfterDelay(succeed bool, delay time.Duration) MessageHandlerFunc {
	return func(ctx context.Context, _ string) error {
		time.Sleep(delay)
		if !succeed {
			return errors.New("an error")
		}
		return nil
	}
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
