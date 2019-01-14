package main

import (
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"os"
)

func main() {
	lambda.Start(HandleRequest)
	//HandleRequest()
}

func HandleRequest(s3Event events.S3Event) {
	pathIncoming := os.Getenv("S3_PATH_INCOMING")
	fmt.Println(pathIncoming)
	pathUncompressed := os.Getenv("S3_PATH_UNCOMPRESSED")
	fmt.Println(pathUncompressed)
	for _, record := range s3Event.Records {
		s3 := record.S3
		fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, s3.Bucket.Name, s3.Object.Key)
	}
}
