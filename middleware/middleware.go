package middleware

import (
	"time"

	"github.com/Wattpad/sqsconsumer"
	"golang.org/x/net/context"
)

// MessageHandlerDecorator is a decorator that can be applied to a handler.
type MessageHandlerDecorator func(sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc

// ApplyDecoratorsToHandler applies all the decorators in inverse order so that d1, d2, d3 results in d3(d2(d1(fn))).
func ApplyDecoratorsToHandler(fn sqsconsumer.MessageHandlerFunc, ds ...MessageHandlerDecorator) sqsconsumer.MessageHandlerFunc {
	for i := len(ds) - 1; i >= 0; i-- {
		fn = ds[i](fn)
	}
	return fn
}

const defaultDeleteBatchTimeout = 250 * time.Millisecond

// DefaultStack returns a base SQS middleware stack which will extend visibility timeouts as long as a handler is running and delete messages on success.
// When the context is cancelled it will clean up the delete queue.
func DefaultStack(ctx context.Context, s *sqsconsumer.SQSService) []MessageHandlerDecorator {
	extend := SQSVisibilityTimeoutExtender(s)
	delete := SQSBatchDeleteOnSuccessWithTimeout(ctx, s, defaultDeleteBatchTimeout)
	return []MessageHandlerDecorator{extend, delete}
}
