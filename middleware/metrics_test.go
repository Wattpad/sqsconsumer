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
	m := TrackMetrics("success", "fail", "timing")

	// when tracking 9 successes, 6 failures, with runtimes in ms of 0, 1, 5, 10, 11 (3 of each)
	for i := 0; i < 3; i++ {
		m(testHandlerReturnAfterDelay(true, 10*time.Millisecond))(context.Background(), "")
		m(testHandlerReturnAfterDelay(true, 20*time.Millisecond))(context.Background(), "")
		m(testHandlerReturnAfterDelay(true, 50*time.Millisecond))(context.Background(), "")
		m(testHandlerReturnAfterDelay(false, 100*time.Millisecond))(context.Background(), "")
		m(testHandlerReturnAfterDelay(false, 110*time.Millisecond))(context.Background(), "")
	}

	// expvar metrics for success and fail counts should match
	assert.Equal(t, "9", expvar.Get("success").String(), "Success count should match")
	assert.Equal(t, "6", expvar.Get("fail").String(), "Failure count should match")

	// expvar metric for timing quantiles 50, 99 should be 5, 11ms
	q50, _ := strconv.Atoi(expvar.Get("timing_p50").String())
	q99, _ := strconv.Atoi(expvar.Get("timing_p99").String())
	assert.InDelta(t, 50, q50, 5, "Time Q50 should match (within 10%)")
	assert.InDelta(t, 110, q99, 11, "Time Q99 should match (within 10%)")
}
