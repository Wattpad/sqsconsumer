# sqsconsumer

[![Build Status](https://travis-ci.org/Wattpad/sqsconsumer.svg?branch=master)](https://travis-ci.org/Wattpad/sqsconsumer)

`sqsconsumer` helps write programs that should respond to messages on an AWS SQS queue. Users of the package only write a processor implementation and then start a consumer bound to a specific queue and processor. The consumer will take care of extending message visibility if the processor takes a long time to run and of only deleting messages from the queue which were successfully processed. Context is passed into the processor so that complex/long workers can gracefully exit when the consumer is interrupted/killed.

See `example/main.go` for a simple demonstration.

# AWS IAM

sqs-consumer requires IAM permissions for the following SQS API actions:

* sqs:ChangeMessageVisibility
* sqs:ChangeMessageVisibilityBatch
* sqs:DeleteMessage
* sqs:DeleteMessageBatch
* sqs:GetQueueAttributes
* sqs:GetQueueUrl
* sqs:ReceiveMessage

# Shutting down gracefully

Canceling the context passed to the `Run` method will propagate cancelation to all running handlers, likely preventing them from completing their task.  To have `sqsconsumer` stop consuming messages while still allowing the in-flight handlers to complete their work, you may provide an optional `shutDown` channel.

Example:

```go
  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()

  shutDown := make(chan struct{})
  // ... Set up shutdown signaling, service, and handler ...
  // shutDown will be closed when it's time to shut down

  c := sqsconsumer.NewConsumer(service, handler)

	wg := &sync.WaitGroup{}
	for i := 0; i < numFetchers; i++ {
    wg.Add(1)
		go func() {
      // Consumer will stop when shutDown is closed
      err := c.Run(ctx, sqsconsumer.WithShutdownChan(shutDown))
      // Handle error
			wg.Done()
		}()
	}

  <-shutDown
  // Force shutdown after deadline
	time.AfterFunc(30*time.Second, cancel)

	// Wait for all the consumers to exit cleanly
	wg.Wait()
```

# TODO

- Terminate ReceiveMessage early if the Context is cancelled during long polling (see https://github.com/aws/aws-sdk-go/issues/75)
