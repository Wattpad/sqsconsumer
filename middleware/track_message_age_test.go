package middleware

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestTrackMessageAge(t *testing.T) {
	ages := new(ages)

	m := TrackMessageAge(10*time.Millisecond, ages.Add)
	fn := m(func(_ context.Context, _ string) error {
		return nil
	})
	ctx := context.Background()

	// When tracking a message with age of 100 seconds delivered the first time
	fn(sqsmessage.NewContext(ctx, newMessageWithAgeAndDeliveryCount(100, 1)), "")

	// wait for an age update
	time.Sleep(15 * time.Millisecond)

	// Should update the age callback with a value between 0 and 100 (because warming up takes time)
	assert.True(t, ages.Len() > 0, "at least one age")
	vals := ages.Values()
	assert.True(t, vals[0] > 0 && vals[0] < 100, "age between 0 and 100 (because warming up takes time)")
}

func TestTrackMessageAgeIgnoresRedelivery(t *testing.T) {
	ages := new(ages)

	m := TrackMessageAge(10*time.Millisecond, ages.Add)
	fn := m(func(_ context.Context, _ string) error {
		return nil
	})
	ctx := context.Background()
	fn(sqsmessage.NewContext(ctx, newMessageWithAgeAndDeliveryCount(100, 2)), "")

	// wait for an age update
	time.Sleep(15 * time.Millisecond)

	assert.True(t, ages.Len() > 0, "at least one age")
	assert.Equal(t, float64(0), ages.Values()[0])
}

func TestTrackMessageAgeApproachesValue(t *testing.T) {
	ages := new(ages)

	m := TrackMessageAge(10*time.Millisecond, ages.Add)
	fn := m(func(_ context.Context, _ string) error {
		return nil
	})
	ctx := context.Background()

	// When tracking many messages with ages of 100 seconds
	msgCtx := sqsmessage.NewContext(ctx, newMessageWithAgeAndDeliveryCount(100, 1))
	for i := 0; i < 2000; i++ {
		fn(msgCtx, "")
		time.Sleep(10 * time.Microsecond)
	}

	vals := ages.Values()
	assert.True(t, len(vals) > 3, "at least 3 ages")
	assert.True(t, vals[0] > 0, "first age greater than 0")
	assert.True(t, vals[1] > vals[0], "second age greater than first")
	assert.True(t, vals[2] > vals[1], "third age greater than second")
	assert.True(t, vals[2] > 90 && vals[2] < 100, "third age between 90 and 100")
}

type ages struct {
	sync.Mutex
	a []float64
}

func (a *ages) Add(age float64) {
	a.Lock()
	a.a = append(a.a, age)
	a.Unlock()
}

func (a *ages) Len() int {
	a.Lock()
	defer a.Unlock()
	return len(a.a)
}

func (a *ages) Values() []float64 {
	a.Lock()
	defer a.Unlock()
	return a.a[:]
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
