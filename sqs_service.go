package sqsconsumer

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Takes SQS type as an argument so the library may be mocked and tested locally
func NewSQSService(queueName string, svc SQSAPI) (*SQSService, error) {

	s := &SQSService{
		Svc:    svc,
		Logger: NoopLogger,
	}

	var url *string
	var err error

	if url, err = SetupQueue(svc, queueName); err != nil {
		return nil, err
	}
	s.URL = url

	return s, nil
}

// SQSServiceForQueue creates an AWS SQS client configured for the given region and gets or creates a queue with the given name.
func SQSServiceForQueue(queueName string, opts ...AWSConfigOption) (*SQSService, error) {
	conf := &aws.Config{}
	for _, o := range opts {
		o(conf)
	}

	svc := sqs.New(session.New(conf))
	return NewSQSService(queueName, svc)
}

type AWSConfigOption func(*aws.Config)

func OptAWSRegion(region string) AWSConfigOption {
	return func(c *aws.Config) {
		c.Region = aws.String(region)
	}
}

// SQSService links an SQS client with a queue URL.
type SQSService struct {
	Svc    SQSAPI
	URL    *string
	Logger func(format string, args ...interface{})
}

// SetupQueue creates the queue to listen on and returns the URL.
func SetupQueue(svc SQSAPI, name string) (*string, error) {
	// if the queue already exists just get the url
	getResp, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})
	if err == nil {
		return getResp.QueueUrl, nil
	}

	// fallback to creating the queue
	createResp, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]*string{
			"MessageRetentionPeriod":        aws.String("1209600"), // 14 days
			"ReceiveMessageWaitTimeSeconds": aws.String("20"),
		},
	})
	if err != nil {
		return nil, err
	}

	return createResp.QueueUrl, nil
}

func NoopLogger(_ string, _ ...interface{}) {}
