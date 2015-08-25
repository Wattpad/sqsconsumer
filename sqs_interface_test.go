package sqsconsumer_test

import (
	"testing"

	"github.com/Wattpad/sqsconsumer"
	"github.com/aws/aws-sdk-go/service/sqs"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSQSInterfaceMatches(t *testing.T) {
	Convey("sqs.New() should implement SQSAPI", t, func() {
		So(sqs.New(nil), ShouldImplement, (*sqsconsumer.SQSAPI)(nil))
	})
}
