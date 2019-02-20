package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHandler(t *testing.T) {
	//err := os.Setenv("SNS_TOPIC_ARN", "arn:aws:sns:eu-west-1:973497026170:man-deveh-address-import-topic")

	var tests = []struct {
		request events.S3Event
		error   error
	}{
		{
			request: events.S3Event{Records: []events.S3EventRecord{{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{
						Name: "man-deveh-stash",
						//Arn:  "arn:aws:s3:::man-deveh-stash",
					},
					Object: events.S3Object{
						//Key: "incoming/Datei2_Haus_2018.csv.gz",
						//Key: "Datei2_Haus_2018_short.csv",
						Key: "Datei2_Haus_2018_short.csv.gz",
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
