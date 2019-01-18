package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"log"
)

func main() {
	iamlocal := false

	if !iamlocal {
		lambda.Start(HandleRequest)
	} else {
		//testRecord := events.S3EventRecord{
		//	EventVersion: "2.0",
		//	EventSource:  "aws:s3",
		//	AWSRegion:    "eu-west-1",
		//	//EventTime: "1970-01-01T00:00:00.000Z",
		//	EventName: "ObjectCreated:Put",
		//	S3: events.S3Entity{
		//		Bucket: events.S3Bucket{
		//			Name: "man-deveh-stash",
		//			Arn:  "arn:aws:s3:::man-deveh-stash",
		//		},
		//		Object: events.S3Object{
		//			Key: "incoming/Datei2_Haus_2018.csv.gz",
		//		},
		//	},
		//}
		//err := HandleRequest(events.SNSEvent{Records: []events.S3EventRecord{testRecord}})
		//if err != nil {
		//	log.Panicf("ein lustiger fehler:\n %s\n", err)
		//}
	}
}

func HandleRequest(snsEvent events.SNSEvent) {
	log.Print("===>>>")
	log.Print(snsEvent)
	log.Print("<<<===")
}
