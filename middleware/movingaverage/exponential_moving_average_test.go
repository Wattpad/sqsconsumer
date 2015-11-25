package movingaverage

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNew(t *testing.T) {
	Convey("New()", t, func() {
		a := New(time.Minute)

		Convey("Should return an *ExponentialMovingAverage", func() {
			So(a, ShouldHaveSameTypeAs, &ExponentialMovingAverage{})

			Convey("With the period set to match the argument, in seconds", func() {
				So(a.period, ShouldAlmostEqual, 60.0)
			})
		})
	})
}

func TestExponentialMovingAverageValue(t *testing.T) {
	Convey("ExponentialMovingAverage.Value()", t, func() {
		Convey("Given a new EMA", func() {
			a := New(time.Nanosecond)

			Convey("Should return 0", func() {
				So(a.Value(), ShouldEqual, 0.0)
			})

			Convey("When the value is set internally", func() {
				a.v = 123.45

				Convey("Should return the specified value", func() {
					So(a.Value(), ShouldAlmostEqual, 123.45)
				})
			})
		})
	})
}

func TestExponentialMovingAverageUpdate(t *testing.T) {
	Convey("ExponentialMovingAverage.Update()", t, func() {
		Convey("Given a new EMA", func() {
			a := New(time.Millisecond)

			Convey("When updated with a zero value", func() {
				a.Update(0.0)

				Convey("Should have a zero value", func() {
					So(a.Value(), ShouldEqual, 0.0)
				})
			})

			Convey("When updated with a non-zero value", func() {
				a.Update(1000.0)

				Convey("Should have a non-zero value that is less than the updated value", func() {
					So(a.Value(), ShouldBeBetween, 0.0, 1000.0)
				})
			})

			Convey("When repeatedly updated with the same non-zero value over time", func() {
				for i := 0; i < 100; i++ {
					a.Update(1000.0)
					time.Sleep(100 * time.Microsecond)
				}

				Convey("Should converge close to that value without reaching it", func() {
					So(a.Value(), ShouldAlmostEqual, 1000.0, 0.1)
					So(a.Value(), ShouldNotAlmostEqual, 1000.0, 0.00000001)
				})
			})
		})
	})
}
