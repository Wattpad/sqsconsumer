package middleware

import (
	"testing"

	"golang.org/x/net/context"
)

func TestUnwrapSNSMessageMiddleware(t *testing.T) {
	testCases := []struct {
		in, ex string
	}{
		{
			in: `{"Type":"Notification","MessageId":"6b7dc12e-af9e-579e-b027-8ff4f76d133c","TopicArn":"arn:aws:sns:us-east-1:723255503624:test_topic","Message":"{\"type\":\"fake_message_type\",\"prop1\":\"val1\"}","Timestamp":"2015-12-03T14:50:27.317Z","SignatureVersion":"1","Signature":"NXAYOAD+tmsCu5+J2vjxNzqZhnL/P5f7LzbvC6qKHo3Swc482C2P8G5rL9k3/VzYZayz9ovHZ+tSjGMpIvrMG5p5iJTtTu5IbBGaQaAP0L9ivRUu5q1Za5Vjdp8R7RZcmCVZfLBKPUx+aqY77O5hWHSOu2YgEC14zBWq9wpkup/syAEgYDsvTLPd6qP6TMpP4okTlPUGpgBkmAWMAQhMwoqVNUin44JDLpRP+x0QJRkOdk5uvBMVuuzOLO26UDWCpeiQHFjpXEdesLBByzIInKR1i85/sRFJxlC2bc5a4OVG974M73i7Cx1Q9B9p14Cl/kfG+j6ku7XT//tJcCYRdg==","SigningCertURL":"https://sns.us-east-1.amazonaws.com/SimpleNotificationService-bb750dd426d95ee9390147a5624348ee.pem","UnsubscribeURL":"https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:723255503624:test_topic:2db8814f-dcaf-477d-babd-93296a0d655a","MessageAttributes":{"AWS.SNS.MOBILE.MPNS.Type":{"Type":"String","Value":"token"},"AWS.SNS.MOBILE.MPNS.NotificationClass":{"Type":"String","Value":"realtime"},"AWS.SNS.MOBILE.WNS.Type":{"Type":"String","Value":"wns/badge"}}}`,
			ex: `{"type":"fake_message_type","prop1":"val1"}`, // note the message quotes are no longer escaped
		},
		{
			in: `{"type":"fake_message_type","prop1":"val1"}`,
			ex: `{"type":"fake_message_type","prop1":"val1"}`,
		},
	}

	for _, tc := range testCases {
		var handler testMessageCapturer
		fn := UnwrapSNSMessage()(handler.handlerFunc)

		fn(context.Background(), tc.in)
		if handler.msg != tc.ex {
			t.Fatalf("UnwrapSNSMessage(%s) result = %s; want %s", tc.in, handler.msg, tc.ex)
		}
	}
}
