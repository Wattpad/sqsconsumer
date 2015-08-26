package sqsconsumer

import (
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
	"golang.org/x/net/context"
)

// TrackMetrics decorates a MessageHandler to collect metrics about successes, failures and runtime reports in ms*10.
func TrackMetrics(successVarName, failureVarName, timingVarName string) MessageHandlerDecorator {
	successes := expvar.NewCounter(successVarName)
	failures := expvar.NewCounter(failureVarName)

	// histogram from 0-100000ms with 3 sigfigs tracking 50, 95 and 99 %iles
	h := expvar.NewHistogram(timingVarName, 0, 100000, 3, 50, 95, 99)

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
