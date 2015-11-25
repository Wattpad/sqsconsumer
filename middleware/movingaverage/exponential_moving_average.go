package movingaverage

import (
	"math"
	"sync"
	"time"
)

// Exponential computes the moving average of a value
type ExponentialMovingAverage struct {
	mu     sync.Mutex
	v      float64
	t      time.Time
	period float64
}

// NewExponentialMovingAverage creates an EMA for a given period
func New(period time.Duration) *ExponentialMovingAverage {
	return &ExponentialMovingAverage{
		mu:     sync.Mutex{},
		t:      time.Now(),
		period: period.Seconds(),
	}
}

// Update computes the next value in a moving average
func (ema *ExponentialMovingAverage) Update(value float64) float64 {
	now := time.Now()
	a := 1.0 - math.Exp(ema.t.Sub(now).Seconds()/ema.period)
	v := a*value + (1.0-a)*ema.v

	ema.mu.Lock()
	ema.v = v
	ema.t = now
	ema.mu.Unlock()

	return ema.v
}

// Value gets the current average
func (ema *ExponentialMovingAverage) Value() float64 {
	ema.mu.Lock()
	defer ema.mu.Unlock()

	return ema.v
}
