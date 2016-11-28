# sqsconsumer

[![Build Status](https://travis-ci.org/Wattpad/sqsconsumer.svg?branch=master)](https://travis-ci.org/Wattpad/sqsconsumer)

`sqsconsumer` helps write programs that should respond to messages on an AWS SQS queue. Users of the package only write a processor implementation and then start a consumer bound to a specific queue and processor. The consumer will take care of extending message visibility if the processor takes a long time to run and of only deleting messages from the queue which were successfully processed. Context is passed into the processor so that complex/long workers can gracefully exit when the consumer is interrupted/killed.

See `example/main.go` for a simple demonstration.

# TODO

- Terminate ReceiveMessage early if the Context is cancelled during long polling (see https://github.com/aws/aws-sdk-go/issues/75)
