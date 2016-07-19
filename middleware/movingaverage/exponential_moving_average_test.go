package movingaverage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	a := New(time.Minute)

	assert.IsType(t, &ExponentialMovingAverage{}, a)
	assert.InDelta(t, 60.0, a.period, 0.01)
}

func TestExponentialMovingAverageValue(t *testing.T) {
	a := New(time.Nanosecond)
	assert.Equal(t, 0.0, a.Value())

	a.v = 123.45
	assert.InDelta(t, 123.45, a.Value(), 0.01)
}

func TestExponentialMovingAverageUpdateZero(t *testing.T) {
	a := New(time.Millisecond)
	a.Update(0.0)
	assert.Equal(t, 0.0, a.Value())
}

func TestExponentialMovingAverageUpdateNonZero(t *testing.T) {
	a := New(time.Millisecond)
	a.Update(1000.0)
	v := a.Value()
	assert.True(t, v > 0.0 && v < 1000.0, "value between 0 and 1000")
}

func TestExponentialMovingAverageUpdateApproachConstant(t *testing.T) {
	a := New(time.Millisecond)
	for i := 0; i < 100; i++ {
		a.Update(1000.0)
		time.Sleep(100 * time.Microsecond)
	}

	v := a.Value()
	assert.InDelta(t, 1000.0, v, 0.1)
	assert.NotEqual(t, 1000.0, v)
}
