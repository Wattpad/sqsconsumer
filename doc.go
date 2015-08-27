// Copyright 2015 WP Technology Inc. All rights reserved.
// Use of this source code is governed by a <TBD>-style
// license that can be found in the LICENSE file.

/*
Package sqsconsumer enables easy and efficient message processing from an SQS queue.

Overview

Consumers will read from queues in batches and run a handler func for each message.
Note that no retry limit is managed by this package, so use the SQS Dead Letter Queue
facility. Of course, you can use another consumer to handle messages that end up in the Dead Letter Queue.

SQS

SQS provides at-least-once delivery with no guarantee of message ordering. When messages are received, a visibility timeout starts and when the timeout expires then the message will be delivered again. Long running message handlers must extend the timeout periodically to ensure that they retain exclusivity on the message, and they must explicitly delete messages that were successfully consumed to avoid redelivery.

To read more about how SQS works, check the SQS documentation at https://aws.amazon.com/documentation/sqs/

Middleware

Visibility timeout extension and deleting messages after successful handling are implemented as handler middleware. See github.com/Wattpad/sqsconsumer/middleware for details on these and other middleware layers available.

Use

See the example directory for a demonstration of use.
*/
package sqsconsumer
