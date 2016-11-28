package sqsconsumer

import "github.com/aws/aws-sdk-go/service/sqs"

// SQSAPI is the part of the AWS SQS API which is used by the sqsconsumer package.
type SQSAPI interface {
	ChangeMessageVisibilityBatch(*sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error)
	CreateQueue(*sqs.CreateQueueInput) (*sqs.CreateQueueOutput, error)
	DeleteMessageBatch(*sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
	GetQueueUrl(*sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error)
	ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
}
