package sqsmessage

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/net/context"
)

// key is a type to prevent collisions with keys in other packages.
type key int

// sqsMessageKey is the context key for the SQS message. Its value is arbitrary, but the type-value combination must be unique.
const sqsMessageKey key = 0

// NewContext returns a new Context carrying msg.
func NewContext(ctx context.Context, msg *sqs.Message) context.Context {
	return context.WithValue(ctx, sqsMessageKey, msg)
}

// FromContext extracts the SQS message from ctx, if present.
func FromContext(ctx context.Context) (*sqs.Message, bool) {
	msg, ok := ctx.Value(sqsMessageKey).(*sqs.Message)
	return msg, ok
}
