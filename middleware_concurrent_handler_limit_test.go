package sqsconsumer

import (
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

func TestWorkerPoolMiddleware(t *testing.T) {
	Convey("ConcurrentHandlerLimit", t, func() {
		Convey("Given a handler that tracks the number of concurrent handlers in flight and blocks until cancelled", func() {
			inFlight := newCountingBlockingHandler()

			Convey("And the handler is decorated with a worker pool configured to limit concurrency to 5", func() {
				fn := ConcurrentHandlerLimit(5)(inFlight.handleMessage)

				Convey("When trying to run 10 handlers at once", func() {
					ctx, cancel := context.WithCancel(context.Background())
					for i := 0; i < 10; i++ {
						go fn(ctx, "a message")
					}

					// and sleeping a moment to give all the handlers time to reach the block
					time.Sleep(time.Millisecond)

					Convey("Then only 5 should be in flight", func() {
						So(inFlight.count(), ShouldBeLessThanOrEqualTo, int64(5))
						cancel()
					})
				})
			})
		})

		Convey("Given two different blocking handlers", func() {
			inFlight1 := newCountingBlockingHandler()
			inFlight2 := newCountingBlockingHandler()

			Convey("And both handlers are decorated with the same worker pool configured to limit concurrency to 5", func() {
				limit := ConcurrentHandlerLimit(5)
				fn1 := limit(inFlight1.handleMessage)
				fn2 := limit(inFlight1.handleMessage)

				Convey("When trying to run 4 handlers of each type at once", func() {
					ctx, cancel := context.WithCancel(context.Background())
					for i := 0; i < 4; i++ {
						go fn1(ctx, "a message")
						go fn2(ctx, "a message")
					}

					// and sleeping a moment to give all the handlers time to reach the block
					time.Sleep(time.Millisecond)

					Convey("Then only 5 should be in flight", func() {
						So(inFlight1.count()+inFlight2.count(), ShouldBeLessThanOrEqualTo, int64(5))
						cancel()
					})
				})
			})
		})
	})
}

func newCountingBlockingHandler() *countingBlockingHandler {
	return &countingBlockingHandler{
		i: new(int64),
	}
}

func (c *countingBlockingHandler) handleMessage(ctx context.Context, _ string) error {
	// increment the in flight counter and decrement when done
	c.incr()
	defer c.decr()

	// block until the context is cancelled
	<-ctx.Done()

	return nil
}

func (c *countingBlockingHandler) incr() {
	atomic.AddInt64(c.i, 1)
}

func (c *countingBlockingHandler) decr() {
	atomic.AddInt64(c.i, -1)
}

func (c *countingBlockingHandler) count() int64 {
	return atomic.LoadInt64(c.i)
}

type countingBlockingHandler struct {
	i *int64
}
