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
	"path/filepath"
	"strconv"
)

type RowObject struct {
	HaID           string
	StID           string
	HaAdrNStrasse  string
	HaAdrNHausnr   string
	HaAdrNHausnrZu string
	HaAdrNPLZ      string
	HaAdrNOTLName  string
	HaAdrNOrtsname string
	HaKooKGenau    string
}

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
	//pathUncompressed := os.Getenv("S3_PATH_UNCOMPRESSED")
	//log.Println("S3_PATH_UNCOMPRESSED", pathUncompressed)
	for _, s3record := range s3Event.Records {
		err := processS3Record(s3record)
		if err != nil {
			return fmt.Errorf("error while processing %s from S3: %s\n", s3record.S3.Object.Key, err)
		}
		err = deleteObjectByRecord(s3record)
		if err != nil {
			return fmt.Errorf("error while deleting %s from S3: %s\n", s3record.S3.Object.Key, err)
		}
	}
	return nil
}

func processS3Record(s3record events.S3EventRecord) error {
	// some output for cloudwatch
	log.Printf("START PROCESSING %s\n", s3record.S3.Object.Key)

	// get object from S3
	obj, err := getObjectByRecord(s3record)
	if err != nil {
		return fmt.Errorf("error while GetObject %s from S3: %s\n", s3record.S3.Object.Key, err)
	}

	// check if it is a *.gz or *.csv
	extension := filepath.Ext(s3record.S3.Object.Key)
	var inputContent io.Reader
	if extension == "gz" {
		gr, err := gzip.NewReader(obj.Body)
		defer gr.Close()
		if err != nil {
			return fmt.Errorf("error while unzipping %s from S3: %s\n", s3record.S3.Object.Key, err)
		}
		inputContent = gr
	} else {
		inputContent = obj.Body
	}

	// build CSV reader
	r := csv.NewReader(bufio.NewReader(inputContent))
	r.Comma = ';'
	r.Comment = '#'

	// go through by lines
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
		rowObject := generateRowObject(record)

		if rowObject.HaID != "" {

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

func getS3Service(region string) *s3.S3 {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	svc := s3.New(sess)
	return svc
}

func getObjectByRecord(s3record events.S3EventRecord) (*s3.GetObjectOutput, error) {
	svc := getS3Service(s3record.AWSRegion)
	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3record.S3.Bucket.Name),
		Key:    aws.String(s3record.S3.Object.Key),
	})
	return obj, err
}

func deleteObjectByRecord(s3record events.S3EventRecord) error {
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

func generateRowObject(record []string) RowObject {
	rowObject := RowObject{
		HaID:           record[0],
		StID:           record[1],
		HaAdrNStrasse:  record[2],
		HaAdrNHausnr:   record[3],
		HaAdrNHausnrZu: record[4],
		HaAdrNPLZ:      record[5],
		HaAdrNOTLName:  record[6],
		HaAdrNOrtsname: record[7],
		HaKooKGenau:    record[8],
	}
	return rowObject
}
