package middleware

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Wattpad/sqsconsumer"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestTrackConsumptionRateMiddleware(t *testing.T) {
	log := &logAccumulator{}
	m := TrackConsumptionRate(log, 100*time.Millisecond, "x%dx")
	fn := m(testConfigurableDelayHandler())
	ctx := context.Background()

	fn(ctx, "125") // 0ms -> 125
	fn(ctx, "50")  // 125 -> 175
	fn(ctx, "50")  // 175 -> 225
	fn(ctx, "25")  // 225 -> 250
	fn(ctx, "25")  // 250 -> 275
	fn(ctx, "50")  // 275 -> 325 (still counts in 3rd period)

	// wait for another log event
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, "x1x", log.Get(0))
	assert.Equal(t, "x2x", log.Get(1))
	assert.Equal(t, "x3x", log.Get(2))
	assert.Equal(t, "x0x", log.Get(3))
}

type logAccumulator struct {
	sync.Mutex
	v []string
}

func (l *logAccumulator) Printf(format string, a ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.v = append(l.v, fmt.Sprintf(format, a...))
}

func (l *logAccumulator) Get(i int) string {
	l.Lock()
	defer l.Unlock()
	return l.v[i]
}

// testConfigurableDelayHandler generates a handler func that will succeed after a delay as specified in the message body
func testConfigurableDelayHandler() sqsconsumer.MessageHandlerFunc {
	return func(_ context.Context, msg string) error {
		// the message is the delay in ms
		delay, err := strconv.ParseInt(msg, 10, 64)
		if err != nil {
			return err
		}

		time.Sleep(time.Duration(delay) * time.Millisecond)
		return nil
	}
}
