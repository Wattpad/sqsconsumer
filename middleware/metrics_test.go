package middleware

import (
	"expvar"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestTrackMetricsMiddleware(t *testing.T) {
	// given a TrackMetrics with known expvar metric names
	successes := expvar.NewInt("success")
	fails := expvar.NewInt("fail")
	timing := expvar.NewFloat("timing")
	m := TrackMetrics(successes, fails, timing)

	// when tracking 9 successes, 6 failures with varied runtimes
	for i := 0; i < 3; i++ {
		m(testHandlerReturnAfterDelay(true, 10*time.Millisecond))(context.Background(), "")
		m(testHandlerReturnAfterDelay(true, 20*time.Millisecond))(context.Background(), "")
		m(testHandlerReturnAfterDelay(true, 50*time.Millisecond))(context.Background(), "")
		m(testHandlerReturnAfterDelay(false, 100*time.Millisecond))(context.Background(), "")
		m(testHandlerReturnAfterDelay(false, 110*time.Millisecond))(context.Background(), "")
	}

	// expvar metrics for success and fail counts should match
	assert.Equal(t, "9", successes.String(), "Success count should match")
	assert.Equal(t, "6", fails.String(), "Failure count should match")

	// expvar metric for timing moving average
	avg, _ := strconv.ParseFloat(timing.String(), 64)
	assert.InDelta(t, 15, avg, 1.5, "Timing average should match (within 10%)")
}
