// Package sqsconsumer enables easy and efficient message processing from an SQS queue.
//
// Consumers will read from queues in batches, extend message visibility timeout as long as the job
// is still running, and delete messages from the queue only when the user-provided handler func completes
// without error. Note that no retry limit is managed by this package, so use the SQS Dead Letter Queue
// facility. Of course, you can use another consumer to handle messages that end up in the Dead Letter Queue.
//
// See the example directory for a demonstration of use.
package sqsconsumer
