package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"io"
	"log"
	"os"
)

var (
	svc s3iface.S3API
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
		err := processS3Record(s3record)
		if err != nil {
			return fmt.Errorf("error while processing %s from S3: %s\n", key, err)
		}
	}
	return nil
}

func processS3Record(s3record events.S3EventRecord) error {
	key := s3record.S3.Object.Key
	log.Println(key)
	log.Println(s3record)

	//dir, file := path.Split(key)
	// Download the file from S3
	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3record.S3.Bucket.Name),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("error in downloading %s from S3: %s\n", key, err)
	}

	//requestInput := s3.GetObjectInput{
	//	Bucket: aws.String(s3record.S3.Bucket.Name),
	//	Key:    aws.String(s3record.S3.Object.Key),
	//}
	//buf := aws.NewWriteAtBuffer([]byte{})

	//body, err := ioutil.ReadAll(obj.Body)
	//if err != nil {
	//	return fmt.Errorf("error in reading file %s: %s\n", key, err)
	//}

	r := csv.NewReader(bufio.NewReader(obj.Body))
	r.Comma = ';'
	r.Comment = '#'

	// per line - generate person and PDF file
	lineCount := 0
	for {
		record, err := r.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}
		// drop first AND second line because of header rows
		if lineCount <= 1 {
			lineCount += 1
			continue
		}
		// map record data to person (good in case of structure change of CSV)
		log.Println(record[0])
		lineCount += 1
	}

	////reader := csv.NewReader(body)
	////record, err := reader.ReadAll()
	////if err != nil {
	////	fmt.Println("Error", err)
	////}
	//
	//reader := csv.NewReader(bytes.NewBuffer(body))
	//record, err := reader.ReadAll()
	//if err != nil {
	//	fmt.Println("Error", err)
	//}
	//
	//for value := range record {
	//	fmt.Println("", record[value])
	//}

	return nil
}
