package middleware

import "github.com/Wattpad/sqsconsumer"

// MessageHandlerDecorator is a decorator that can be applied to a handler.
type MessageHandlerDecorator func(sqsconsumer.MessageHandlerFunc) sqsconsumer.MessageHandlerFunc

// ApplyDecoratorsToHandler applies all the decorators in inverse order so that d1, d2, d3 results in d3(d2(d1(fn))).
func ApplyDecoratorsToHandler(fn sqsconsumer.MessageHandlerFunc, ds ...MessageHandlerDecorator) sqsconsumer.MessageHandlerFunc {
	for i := len(ds) - 1; i >= 0; i-- {
		fn = ds[i](fn)
	}
	return fn
}
