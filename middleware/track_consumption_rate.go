package middleware

import (
	"sync/atomic"
	"time"

	"github.com/Wattpad/sqsconsumer"
	"golang.org/x/net/context"
)

type Logger interface {
	Printf(format string, a ...interface{})
}

// TrackConsumptionRate invokes the logger once per specified period with the format and the int64 number of requests consumed during the period
func TrackConsumptionRate(log Logger, period time.Duration, format string) MessageHandlerDecorator {
	var count int64

	go func() {
		for {
			<-time.After(period)
			c := atomic.SwapInt64(&count, 0)
			log.Printf(format, c)
		}
	}()

	return func(fn sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc {
		return func(ctx context.Context, msg string) error {
			atomic.AddInt64(&count, 1)

			return fn(ctx, msg)
		}
	}
}
