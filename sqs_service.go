package sqsconsumer

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// SQSServiceForQueue creates an AWS SQS client configured for the given region and gets or creates a queue with the given name.
func SQSServiceForQueue(queueName string, opts ...AWSConfigOption) (*SQSService, error) {
	conf := &aws.Config{}
	for _, o := range opts {
		o(conf)
	}

	svc := sqs.New(conf)
	s := &SQSService{Svc: svc}

	var url *string
	var err error
	if url, err = SetupQueue(svc, queueName); err != nil {
		return nil, err
	}
	s.URL = url

	return s, nil
}

type AWSConfigOption func(*aws.Config)

func OptAWSRegion(region string) AWSConfigOption {
	return func(c *aws.Config) {
		c.Region = aws.String(region)
	}
}

// SQSService links an SQS client with a queue URL.
type SQSService struct {
	Svc SQSAPI
	URL *string
}

// SetupQueue creates the queue to listen on and returns the URL.
func SetupQueue(svc SQSAPI, name string) (*string, error) {
	// if the queue already exists just get the url
	getResp, err := svc.GetQueueURL(&sqs.GetQueueURLInput{
		QueueName: aws.String(name),
	})
	if err == nil {
		return getResp.QueueURL, nil
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

	return createResp.QueueURL, nil
}
