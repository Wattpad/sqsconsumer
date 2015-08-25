package sqsconsumer

import "golang.org/x/net/context"

// MessageHandlerFunc is the interface that users of this library should implement. It will be called once per message and should return an error if there was a problem processing the message. When an error is returned, the message will be left on the queue for re-processing.
type MessageHandlerFunc func(ctx context.Context, msg string) error

// MessageHandlerDecorator is a decorator that can be applied to a handler
type MessageHandlerDecorator func(MessageHandlerFunc) MessageHandlerFunc

// ApplyDecoratorsToHandler applies all the decorators in inverse order so that d1, d2, d3 results in d3(d2(d1(fn)))
func ApplyDecoratorsToHandler(fn MessageHandlerFunc, ds ...MessageHandlerDecorator) MessageHandlerFunc {
	for i := len(ds) - 1; i >= 0; i-- {
		fn = ds[i](fn)
	}
	return fn
}
