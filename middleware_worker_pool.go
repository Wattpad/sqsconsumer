package sqsconsumer

import "golang.org/x/net/context"

// WorkerPoolMiddleware decorates a MessageHandler to limit the number of handlers running at a time
func WorkerPoolMiddleware(limit int) MessageHandlerDecorator {
	return func(fn MessageHandlerFunc) MessageHandlerFunc {
		pool := newHandlerPool(limit, fn)

		return func(ctx context.Context, msg string) error {
			var err error

			select {
			// get a handler from the pool
			case fn := <-pool:
				err = fn(ctx, msg)

			// context cancelled
			case <-ctx.Done():
				err = ctx.Err()
			}

			// return the handler to the pool
			pool <- fn

			return err
		}
	}
}

type handlerPool chan MessageHandlerFunc

func newHandlerPool(size int, fn MessageHandlerFunc) handlerPool {
	p := make(chan MessageHandlerFunc, size)
	for i := 0; i < size; i++ {
		p <- fn
	}
	return p
}
