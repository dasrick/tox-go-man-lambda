package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"strconv"
)

func main() {
	//lambda.Start(HandleRequest)

	testRecord := events.S3EventRecord{
		EventVersion: "2.0",
		EventSource:  "aws:s3",
		AWSRegion:    "eu-west-1",
		//EventTime: "1970-01-01T00:00:00.000Z",
		EventName: "ObjectCreated:Put",
		S3: events.S3Entity{
			Bucket: events.S3Bucket{
				Name: "man-deveh-stash",
				Arn:  "arn:aws:s3:::man-deveh-stash",
			},
			Object: events.S3Object{
				Key: "incoming/Datei2_Haus_2018.csv.gz",
			},
		},
	}
	err := HandleRequest(events.S3Event{Records: []events.S3EventRecord{testRecord}})
	if err != nil {
		log.Panicf("ein lustiger fehler:\n %s\n", err)
	}
}

func HandleRequest(s3Event events.S3Event) error {
	//pathIncoming := os.Getenv("S3_PATH_INCOMING")
	//log.Println("S3_PATH_INCOMING", pathIncoming)
	//pathUncompressed := os.Getenv("S3_PATH_UNCOMPRESSED")
	//log.Println("S3_PATH_UNCOMPRESSED", pathUncompressed)
	for _, s3record := range s3Event.Records {
		err := processS3Record(s3record)
		if err != nil {
			return fmt.Errorf("error while processing %s from S3: %s\n", s3record.S3.Object.Key, err)
		}
	}
	return nil
}

func processS3Record(s3record events.S3EventRecord) error {
	// define some useful shorter vars
	key := s3record.S3.Object.Key
	bucket := s3record.S3.Bucket.Name
	region := s3record.AWSRegion

	// some output for cloudwatch
	log.Printf("start processing %s\n", key)

	// create session
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	svc := s3.New(sess)

	// get object from S3
	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("error while GetObject %s from S3: %s\n", key, err)
	}

	gr, err := gzip.NewReader(obj.Body)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	// build CSV reader
	r := csv.NewReader(bufio.NewReader(gr))
	r.Comma = ';'
	r.Comment = '#'

	// go throug by lines
	lineCount := 0
	for {
		record, err := r.Read()
		// Stop at EOF.
		if err == io.EOF {
			log.Println("End of File")
			break
		}
		// drop first line because of header rows
		if lineCount <= 0 {
			lineCount++
			log.Println("Skip header row")
			continue
		}
		// generate object from record
		//rowObject := generateRowObject(record)
		//log.Println(rowObject.HaID)
		//log.Println(record[0])
		if record[0] != "" {

		}
		// now its time to do something with this object

		// ...
		lineCount++
	}

	// some output for cloudwatch
	log.Printf("processed lines %s\n", strconv.Itoa(lineCount))
	log.Printf("finish processing %s\n", key)

	return nil
}
