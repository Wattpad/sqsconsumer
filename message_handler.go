package sqsconsumer

import "golang.org/x/net/context"

// MessageHandlerFunc is the interface that users of this library should implement. It will be called once per message and should return an error if there was a problem processing the message. When an error is returned, the message will be left on the queue for re-processing.
type MessageHandlerFunc func(ctx context.Context, msg string) error
