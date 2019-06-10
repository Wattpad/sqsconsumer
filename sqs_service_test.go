package sqsconsumer

import (
	"testing"

	"github.com/Wattpad/sqsconsumer/mock"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestSetupQueueExists(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	name := "fake_queue_name"

	svc := mock.NewMockSQSAPI(ctl)
	svc.EXPECT().GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(name)}).Return(&sqs.GetQueueUrlOutput{QueueUrl: aws.String("http://example.com/queue/" + name)}, nil)

	url, err := SetupQueue(svc, name)

	assert.Regexp(t, "/fake_queue_name$", *url)
	assert.Nil(t, err)
}

func TestSetupQueue(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	name := "fake_queue_name"

	svc := mock.NewMockSQSAPI(ctl)

	// Given SQS creates the new queue successfully
	svc.EXPECT().GetQueueUrl(gomock.Any()).Return(nil, assert.AnError)
	svc.EXPECT().CreateQueue(gomock.Any()).Return(&sqs.CreateQueueOutput{QueueUrl: aws.String("http://example.com/queue/" + name)}, nil)

	url, err := SetupQueue(svc, name)

	assert.Regexp(t, "/fake_queue_name$", *url)
	assert.Nil(t, err)
}

func TestSetupQueueFails(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	name := "fake_queue_name"

	svc := mock.NewMockSQSAPI(ctl)

	// Given SQS returns an error
	svc.EXPECT().GetQueueUrl(gomock.Any()).Return(nil, assert.AnError)
	svc.EXPECT().CreateQueue(gomock.Any()).Return(nil, assert.AnError)

	url, err := SetupQueue(svc, name)

	assert.Nil(t, url)

	assert.NotNil(t, err)
}
