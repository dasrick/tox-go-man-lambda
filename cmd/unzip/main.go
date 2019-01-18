package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"strconv"
)

func main() {
	iamlocal := false

	if !iamlocal {
		lambda.Start(HandleRequest)
	} else {
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
		err = deleteS3Record(s3record)
		if err != nil {
			return fmt.Errorf("error while deleting %s from S3: %s\n", s3record.S3.Object.Key, err)
		}
	}
	return nil
}

func processS3Record(s3record events.S3EventRecord) error {
	// some output for cloudwatch
	log.Printf("START PROCESSING %s\n", s3record.S3.Object.Key)

	// create s3service and session
	svc := getS3Service(s3record.AWSRegion)

	// get object from S3
	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3record.S3.Bucket.Name),
		Key:    aws.String(s3record.S3.Object.Key),
	})
	if err != nil {
		return fmt.Errorf("error while GetObject %s from S3: %s\n", s3record.S3.Object.Key, err)
	}

	gr, err := gzip.NewReader(obj.Body)
	defer gr.Close()
	if err != nil {
		return fmt.Errorf("error while unzipping %s from S3: %s\n", s3record.S3.Object.Key, err)
	}

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
			log.Println("END of FILE")
			break
		}
		// drop first line because of header rows
		if lineCount <= 0 {
			lineCount++
			log.Println("SKIP HEADER ROW")
			continue
		}
		// generate object from record
		//rowObject := generateRowObject(record)
		//log.Println(rowObject.HaID)
		//log.Println(record[0])
		if record[0] != "" {

		}
		// now its time to do something with this object
		// a good idea would be a SNS event

		// ...
		lineCount++
	}

	// some output for cloudwatch
	log.Printf("PROCESSED LINES %s\n", strconv.Itoa(lineCount))
	log.Printf("FINISH PROCESSING %s\n", s3record.S3.Object.Key)

	return nil
}

func deleteS3Record(s3record events.S3EventRecord) error {
	// create s3service and session
	svc := getS3Service(s3record.AWSRegion)

	// delete processed s3record
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s3record.S3.Bucket.Name),
		Key:    aws.String(s3record.S3.Object.Key),
	})
	if err != nil {
		return err
	}
	return nil
}

func getS3Service(region string) *s3.S3 {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	svc := s3.New(sess)
	return svc
}
