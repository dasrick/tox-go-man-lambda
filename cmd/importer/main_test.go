package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestHandler(t *testing.T) {
	_ = os.Setenv("SNS_TOPIC_ARN", "arn:aws:sns:eu-west-1:973497026170:man-deveh-address-import-topic")

	var tests = []struct {
		request events.S3Event
		error   error
	}{
		{
			request: events.S3Event{Records: []events.S3EventRecord{{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{
						Name: "man-deveh-stash",
					},
					Object: events.S3Object{
						Key: "Datei2_Haus_2018_short_lines_2.csv",
					},
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
