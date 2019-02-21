package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestHandler(t *testing.T) {
	_ = os.Setenv("DYNAMODB_SOURCE_NAME", "man-deveh-source")

	var tests = []struct {
		request events.SNSEvent
		error   error
	}{
		{
			request: events.SNSEvent{Records: []events.SNSEventRecord{{
				EventVersion: "1.0",
				EventSource:  "aws:sns",
				SNS: events.SNSEntity{
					Type:    "Notification",
					Message: `{"HaID":"010530410005100006","StID":"0105304100051","HaAdrNStrasse":"Rehhagen","HaAdrNHausnr":"6","HaAdrNHausnrZu":"","HaAdrNPLZ":"23627","HaAdrNOTLName":"","HaAdrNOrtsname":"Groß Grönau","HaKooKGenau":"Adresspunkt"}`,
				},
			}}},
			error: nil,
		},
	}
	for _, test := range tests {
		err := HandleRequest(test.request)
		assert.IsType(t, test.error, err)
	}
}
