package sqsconsumer

type runOpts struct {
	shutDown <-chan struct{}
}

type RunOption func(o *runOpts)

// WithShutdownChan accepts a channel that will gracefully shut down
// the consumer when it is closed.  The consumer will stop receiving messages from SQS
// and will return from the Run method once in-flight handlers have completed.
//
// The channel must be closed for shutdown to occur.  Sending a value down the channel
// will not shut down the consumer.
//
// The Run method's context.Context may be canceled during this time to abort pending
// operations early. This is done co-operatively and requires the consumer's handler func
// to honour the context cancelation.
func WithShutdownChan(shutDown <-chan struct{}) RunOption {
	return func(o *runOpts) { o.shutDown = shutDown }
}
