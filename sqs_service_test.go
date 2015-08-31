package sqsconsumer

import (
	"testing"

	"github.com/Wattpad/sqsconsumer/mock"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestSetupQueue(t *testing.T) {
	Convey("SetupQueue()", t, func() {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		name := "fake_queue_name"

		svc := mock.NewMockSQSAPI(ctl)

		Convey("Given SQS will return that the queue already exists", func() {
			svc.EXPECT().GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(name)}).Return(&sqs.GetQueueUrlOutput{QueueUrl: aws.String("http://example.com/queue/" + name)}, nil)

			Convey("When SetupQueue is invoked", func() {
				url, err := SetupQueue(svc, name)

				Convey("Then the result will be a URL that ends with the given queue name", func() {
					So(*url, ShouldEndWith, "/fake_queue_name")
				})

				Convey("And no error", func() {
					So(err, ShouldBeNil)
				})
			})
		})

		Convey("Given SQS creates the new queue successfully", func() {
			svc.EXPECT().GetQueueUrl(gomock.Any()).Return(nil, assert.AnError)
			svc.EXPECT().CreateQueue(gomock.Any()).Return(&sqs.CreateQueueOutput{QueueUrl: aws.String("http://example.com/queue/" + name)}, nil)

			Convey("When SetupQueue is invoked", func() {
				url, err := SetupQueue(svc, name)

				Convey("Then the result will be a URL that ends with the given queue name", func() {
					So(*url, ShouldEndWith, "/fake_queue_name")
				})

				Convey("And no error", func() {
					So(err, ShouldBeNil)
				})
			})
		})

		Convey("Given SQS returns an error", func() {
			svc.EXPECT().GetQueueUrl(gomock.Any()).Return(nil, assert.AnError)
			svc.EXPECT().CreateQueue(gomock.Any()).Return(nil, assert.AnError)

			Convey("When SetupQueue is invoked", func() {
				url, err := SetupQueue(svc, name)

				Convey("Then the result will be a nil URL", func() {
					So(url, ShouldBeNil)
				})

				Convey("And a non-nil error", func() {
					So(err, ShouldNotBeNil)
				})
			})
		})
	})
}
