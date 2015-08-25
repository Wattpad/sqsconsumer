package sqsconsumer

import (
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
	"golang.org/x/net/context"
)

// TrackMetricsMiddleware decorates a MessageHandler to collect metrics about successes, failures and runtime reports in ms*10
func TrackMetricsMiddleware(successVarName, failureVarName, timingVarName string) MessageHandlerDecorator {
	successes := expvar.NewCounter(successVarName)
	failures := expvar.NewCounter(failureVarName)
	h := expvar.NewHistogram(timingVarName, 0, 100000, 3, 1, 5, 50, 95, 99)

	timing := metrics.NewTimeHistogram(time.Millisecond, h)

	return func(fn MessageHandlerFunc) MessageHandlerFunc {
		return func(ctx context.Context, msg string) error {
			start := time.Now()
			defer func() {
				timing.Observe(time.Since(start))
			}()

			err := fn(ctx, msg)
			if err != nil {
				failures.Add(1)
			} else {
				successes.Add(1)
			}
			return err
		}
	}
}
