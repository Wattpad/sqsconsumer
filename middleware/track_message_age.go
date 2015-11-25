package middleware

import (
	"errors"
	"strconv"
	"time"

	"github.com/Wattpad/sqsconsumer"
	"github.com/Wattpad/sqsconsumer/middleware/movingaverage"
	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"golang.org/x/net/context"
)

// TrackMessageAge is middleware that tracks the exponential moving average of message age, calling a callback function with the current average periodically
func TrackMessageAge(period time.Duration, f func(age float64)) MessageHandlerDecorator {
	ema := movingaverage.New(period)

	go func() {
		for {
			<-time.After(period)
			f(ema.Value())
		}
	}()

	return func(fn sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc {
		return func(ctx context.Context, msg string) error {
			age, err := computeMessageAge(ctx)
			if err == nil {
				ema.Update(age)
			}

			return fn(ctx, msg)
		}
	}
}

var errCannotComputeAge = errors.New("cannot compute message age")

const (
	nanosPerMilli = 1e6
	millisPerSec  = 1e3
)

// computeMessageAge computes the age in seconds of a message
func computeMessageAge(ctx context.Context) (float64, error) {
	m, ok := sqsmessage.FromContext(ctx)
	if !ok {
		return 0, errCannotComputeAge
	}

	// only count the first receipt
	receiveCount, ok := m.Attributes["ApproximateReceiveCount"]
	if !ok {
		return 0, errCannotComputeAge
	}

	rc, err := strconv.ParseInt(*receiveCount, 10, 64)
	if err != nil {
		return 0, errCannotComputeAge
	}
	if rc > 1 {
		return 0, errCannotComputeAge
	}

	sentTimestamp, ok := m.Attributes["SentTimestamp"]
	if !ok || sentTimestamp == nil {
		return 0, errCannotComputeAge
	}

	s, err := strconv.ParseInt(*sentTimestamp, 10, 64)
	if err != nil {
		return 0, errCannotComputeAge
	}

	now := time.Now().UnixNano() / nanosPerMilli
	ageSecs := (float64)(now-s) / millisPerSec

	return ageSecs, nil
}
