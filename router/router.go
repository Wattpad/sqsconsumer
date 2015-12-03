package router

import (
	"encoding/json"
	"fmt"

	"github.com/Wattpad/sqsconsumer"
	"golang.org/x/net/context"
)

// Type is a router from type value to a handler func
type Type map[string]sqsconsumer.MessageHandlerFunc

// New makes a new router
func New() Type {
	return make(Type)
}

// Add registers a handler func for a specific message type
func (t Type) Add(r string, h sqsconsumer.MessageHandlerFunc) {
	t[r] = h
}

// Handler handles JSON messages and routes the message to an appropriate handler func based on the "type" property of the message
func (t Type) Handler(ctx context.Context, msg string) error {
	var tm typedMessage
	err := json.Unmarshal([]byte(msg), &tm)
	if err != nil {
		return err
	}
	if tm.Type == "" {
		return RouteNotFoundError{tm.Type}
	}

	fn, ok := t[tm.Type]
	if !ok {
		return RouteNotFoundError{tm.Type}
	}

	return fn(ctx, msg)
}

// RouteNotFoundError is an error that includes the type that did not match any route
type RouteNotFoundError struct {
	t string
}

func (e RouteNotFoundError) Error() string {
	return fmt.Sprintf("No route found for type: %s", e.t)
}

type typedMessage struct {
	Type string `json:"type"`
}
