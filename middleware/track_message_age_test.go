package middleware

import (
	"fmt"
	"testing"
	"time"

	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

func TestTrackMessageAge(t *testing.T) {
	Convey("TrackMessageAge()", t, func() {
		Convey("Given a TrackMessageAge with a callback that records updates", func() {
			var ages []float64
			cb := func(age float64) {
				ages = append(ages, age)
			}

			m := TrackMessageAge(10*time.Millisecond, cb)
			fn := m(func(_ context.Context, _ string) error {
				return nil
			})
			ctx := context.Background()

			Convey("When tracking a message with age of 100 seconds delivered the first time", func() {
				fn(sqsmessage.NewContext(ctx, newMessageWithAgeAndDeliveryCount(100, 1)), "")

				// wait for an age update
				time.Sleep(15 * time.Millisecond)

				Convey("Should update the age callback with a value between 0 and 100 (because warming up takes time)", func() {
					So(len(ages), ShouldBeGreaterThan, 0)
					So(ages[0], ShouldBeBetween, 0, 100)
				})
			})

			Convey("When tracking a message with age of 100 seconds being re-delivered", func() {
				fn(sqsmessage.NewContext(ctx, newMessageWithAgeAndDeliveryCount(100, 2)), "")

				// wait for an age update
				time.Sleep(15 * time.Millisecond)

				Convey("Should ignore the message and update the age callback with a value of 0", func() {
					So(len(ages), ShouldBeGreaterThan, 0)
					So(ages[0], ShouldEqual, 0)
				})
			})

			Convey("When tracking many messages with ages of 100 seconds", func() {
				msgCtx := sqsmessage.NewContext(ctx, newMessageWithAgeAndDeliveryCount(100, 1))
				for i := 0; i < 2000; i++ {
					fn(msgCtx, "")
					time.Sleep(10 * time.Microsecond)
				}

				Convey("Should update the age callback with values progressively closer to 100", func() {
					So(len(ages), ShouldBeGreaterThan, 3)
					So(ages[0], ShouldBeGreaterThan, 0)
					So(ages[1], ShouldBeGreaterThan, ages[0])
					So(ages[2], ShouldBeGreaterThan, ages[1])
					So(ages[2], ShouldBeBetween, 90, 100)
				})
			})
		})
	})
}

const (
	nanosPerSec = 1e9
)

func newMessageWithAgeAndDeliveryCount(age, count int64) *sqs.Message {
	// SentTimestamp should be a unix timestamp in milliseconds
	t := (time.Now().UnixNano() - age*nanosPerSec) / nanosPerMilli

	return &sqs.Message{
		Attributes: map[string]*string{
			"SentTimestamp":           aws.String(fmt.Sprintf("%d", t)),
			"ApproximateReceiveCount": aws.String(fmt.Sprintf("%d", count)),
		},
		Body: aws.String("{}"),
	}
}
