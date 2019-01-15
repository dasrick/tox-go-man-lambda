package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"log"
	"os"
)

func main() {
	lambda.Start(HandleRequest)
	//HandleRequest()
}

func HandleRequest(s3Event events.S3Event) error {

	pathIncoming := os.Getenv("S3_PATH_INCOMING")
	log.Println(pathIncoming)

	pathUncompressed := os.Getenv("S3_PATH_UNCOMPRESSED")
	log.Println(pathUncompressed)

	for _, s3record := range s3Event.Records {
		key := s3record.S3.Object.Key
		log.Println(key)
		processS3Record(s3record)
	}
	return nil
}

func processS3Record(s3record events.S3EventRecord) {
	key := s3record.S3.Object.Key
	log.Println(key)
	log.Println(s3record)
}
