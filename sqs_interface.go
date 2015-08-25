package sqsconsumer

import "github.com/aws/aws-sdk-go/service/sqs"

// SQSAPI is the part of the AWS SQS API which is used by the sqsconsumer package
type SQSAPI interface {
	ChangeMessageVisibility(*sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error)
	CreateQueue(*sqs.CreateQueueInput) (*sqs.CreateQueueOutput, error)
	DeleteMessageBatch(*sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
	GetQueueURL(*sqs.GetQueueURLInput) (*sqs.GetQueueURLOutput, error)
	ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
}

// SQSService links an SQS client with a queue URL
type SQSService struct {
	Svc SQSAPI
	URL *string
}
