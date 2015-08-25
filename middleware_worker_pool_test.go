package sqsconsumer

import (
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

func TestWorkerPoolMiddleware(t *testing.T) {
	Convey("WorkerPoolMiddleware", t, func() {
		Convey("Given a handler that tracks the number of concurrent handlers in flight and blocks until cancelled", func() {
			var inFlight int64
			fn := func(ctx context.Context, _ string) error {
				// increment the in flight counter and decrement when done
				atomic.AddInt64(&inFlight, 1)
				defer atomic.AddInt64(&inFlight, -1)

				// block until the context is cancelled
				<-ctx.Done()

				return nil
			}

			Convey("And the handler is decorated with a worker pool configured to limit concurrency to 5", func() {
				fn = WorkerPoolMiddleware(5)(fn)

				Convey("When trying to run 10 handlers at once", func() {
					ctx, cancel := context.WithCancel(context.Background())
					for i := 0; i < 10; i++ {
						go fn(ctx, "a message")
					}

					// and sleeping a moment to give all the handlers time to reach the block
					time.Sleep(time.Millisecond)

					Convey("Then only 5 should be in flight", func() {
						So(inFlight, ShouldBeLessThanOrEqualTo, int64(5))
						cancel()
					})
				})
			})
		})
	})
}
