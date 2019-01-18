package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
)

func main() {
	lambda.Start(HandleRequest)
	//HandleRequest(events.S3Event{
	//	Records:
	//})
}

func HandleRequest(s3Event events.S3Event) error {

	log.Println(s3Event.Records)

	pathIncoming := os.Getenv("S3_PATH_INCOMING")
	log.Println("S3_PATH_INCOMING", pathIncoming)

	pathUncompressed := os.Getenv("S3_PATH_UNCOMPRESSED")
	log.Println("S3_PATH_UNCOMPRESSED", pathUncompressed)

	for _, s3record := range s3Event.Records {
		err := processS3Record(s3record)
		if err != nil {
			return fmt.Errorf("error while processing %s from S3: %s\n", s3record.S3.Object.Key, err)
		}
	}
	return nil
}

func processS3Record(s3record events.S3EventRecord) error {
	key := s3record.S3.Object.Key
	bucket := s3record.S3.Bucket.Name

	log.Println(key)
	log.Println(bucket)

	// Create Session || Create a S3 client instance from a session
	sess := session.Must(session.NewSession())

	svc := s3.New(sess)

	//log.Println(svc)
	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("error in downloading %s from S3: %s\n", key, err)
	}

	r := csv.NewReader(bufio.NewReader(obj.Body))
	r.Comma = ';'
	r.Comment = '#'

	return nil
}
