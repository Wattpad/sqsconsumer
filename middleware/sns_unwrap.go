package middleware

import (
	"encoding/json"

	"github.com/Wattpad/sqsconsumer"
	"golang.org/x/net/context"
)

// UnwrapSNSMessage decorates a MessageHandler to unwrap messages sent via SNS
func UnwrapSNSMessage() MessageHandlerDecorator {
	return func(fn sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc {
		return func(ctx context.Context, msg string) error {
			var e snsEnvelope
			err := json.Unmarshal([]byte(msg), &e)
			if err == nil && isSNSMessage(e) {
				return fn(ctx, e.Message)
			}

			// if the message could not be unwrapped it may not have come through SNS so just passthrough
			return fn(ctx, msg)
		}
	}
}

type snsEnvelope struct {
	Type, TopicArn, MessageID, Message string
}

func isSNSMessage(e snsEnvelope) bool {
	return e.TopicArn != "" && e.MessageID != "" && e.Type == "Notification"
}
