package middleware

import (
	"github.com/Wattpad/sqsconsumer"
	"golang.org/x/net/context"
)

// ConcurrentHandlerLimit decorates a MessageHandler to limit the number of handlers running at a time.
//
// Given that the SQS consumer only handles up to 10 messages at a time anyway, there's probably no need for this if you
// use only one consumer, but if you have many concurrent consumers and you want to limit the number of message handlers
// in flight across the set of consumers, create one ConcurrentHandlerLimit middleware and apply the same one to all the
// consumers' handler funcs.
func ConcurrentHandlerLimit(limit int) MessageHandlerDecorator {
	// close over the pool so that one limit can apply to multiple handlers if so desired
	pool := newTokenPool(limit)

	return func(fn sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc {
		return func(ctx context.Context, msg string) error {
			var err error

			select {
			// get a token from the pool
			case <-pool:
				err = fn(ctx, msg)
				pool <- struct{}{}

			// context cancelled
			case <-ctx.Done():
				err = ctx.Err()
			}

			return err
		}
	}
}

func newTokenPool(size int) chan struct{} {
	p := make(chan struct{}, size)
	for i := 0; i < size; i++ {
		p <- struct{}{}
	}
	return p
}
