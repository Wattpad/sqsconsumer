package sqsconsumer

import "golang.org/x/net/context"

// ConcurrentHandlerLimit decorates a MessageHandler to limit the number of handlers running at a time.
func ConcurrentHandlerLimit(limit int) MessageHandlerDecorator {
	// close over the pool so that one limit can apply to multiple handlers if so desired
	pool := newTokenPool(limit)

	return func(fn MessageHandlerFunc) MessageHandlerFunc {
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
