package sqsconsumer_test

import (
	"testing"

	"github.com/Wattpad/sqsconsumer"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
)

func TestSQSInterfaceImplementsSQSAPI(t *testing.T) {
	sess, err := session.NewSession()
	assert.NoError(t, err)
	assert.Implements(t, (*sqsconsumer.SQSAPI)(nil), sqs.New(sess))
}
