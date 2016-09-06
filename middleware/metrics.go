package middleware

import (
	"expvar"
	"time"

	"github.com/Wattpad/sqsconsumer"
	"github.com/Wattpad/sqsconsumer/middleware/movingaverage"
	"golang.org/x/net/context"
)

// TrackMetrics decorates a MessageHandler to collect metrics about successes, failures and runtime reports in ms*10.
func TrackMetrics(successes, failures *expvar.Int, timing *expvar.Float) MessageHandlerDecorator {
	ema := movingaverage.New(5 * time.Second)

	return func(fn sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc {
		return func(ctx context.Context, msg string) error {
			start := time.Now()
			defer func() {
				v := ema.Update(time.Since(start).Seconds() * 1000)
				timing.Set(v)
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
