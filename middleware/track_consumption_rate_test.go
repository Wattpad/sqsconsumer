package middleware

import (
	"testing"

	"time"

	"strconv"

	"fmt"

	"github.com/Wattpad/sqsconsumer"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

func TestTrackConsumptionRateMiddleware(t *testing.T) {
	Convey("TrackConsumptionRate()", t, func() {
		Convey("Given a TrackConsumptionRate with a logger that just stores the output", func() {
			log := &logAccumulator{}
			m := TrackConsumptionRate(log, 100*time.Millisecond, "x%dx")
			fn := m(testConfigurableDelayHandler())
			ctx := context.Background()

			Convey("When tracking messages that occur 1 in the first period, 2 in the second and 3 in the third", func() {
				fn(ctx, "125") // 0ms -> 125
				fn(ctx, "50")  // 125 -> 175
				fn(ctx, "50")  // 175 -> 225
				fn(ctx, "25")  // 225 -> 250
				fn(ctx, "25")  // 250 -> 275
				fn(ctx, "50")  // 275 -> 325 (still counts in 3rd period)

				// wait for another log event
				time.Sleep(100 * time.Millisecond)

				Convey("Should log counts of 1, 2, 3 and 0", func() {
					So((*log)[0], ShouldEqual, "x1x")
					So((*log)[1], ShouldEqual, "x2x")
					So((*log)[2], ShouldEqual, "x3x")
					So((*log)[3], ShouldEqual, "x0x")
				})
			})
		})
	})
}

type logAccumulator []string

func (l *logAccumulator) Printf(format string, a ...interface{}) {
	*l = append(*l, fmt.Sprintf(format, a...))
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
