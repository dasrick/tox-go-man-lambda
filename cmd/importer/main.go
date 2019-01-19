package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
	"io"
	"log"
	"os"
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
		err := os.Setenv("SNS_TOPIC_ARN", "arn:aws:sns:eu-west-1:973497026170:man-deveh-address-import-topic")
		if err != nil {
			log.Panicf("ein lustiger fehler:\n %s\n", err)
		}
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
					//Key: "incoming/Datei2_Haus_2018.csv.gz",
					//Key: "Datei2_Haus_2018_short.csv",
					Key: "Datei2_Haus_2018_short.csv.gz",
				},
			},
		}
		err = HandleRequest(events.S3Event{Records: []events.S3EventRecord{testRecord}})
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
	var inputContent io.Reader
	if filepath.Ext(s3record.S3.Object.Key) == ".gz" {
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
		//rowObject := generateRowObject(record)
		//if rowObject.HaID != "" {
		//
		//}
		// now its time to do something with this object
		// a good idea would be a SNS event
		_, err = publishByRecord(s3record, record)
		if err != nil {
			return fmt.Errorf("error while publishing record to SNS: %s\n", err)
		}
		//log.Println(resp)

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

func getSNSService(region string) *sns.SNS {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	svc := sns.New(sess)
	return svc
}

func publishByRecord(s3record events.S3EventRecord, row []string) (*sns.PublishOutput, error) {
	rowObject := generateRowObject(row)
	rowJSON, err := json.Marshal(rowObject)
	if err != nil {
		return nil, fmt.Errorf("error while json.Marshal %s : %s\n", s3record.S3.Object.Key, err)
	}
	// create s3service and session
	svc := getSNSService(s3record.AWSRegion)
	params := &sns.PublishInput{
		Message:  aws.String(string(rowJSON)), // This is the message itself (can be XML / JSON / Text - anything you want)
		TopicArn: aws.String(os.Getenv("SNS_TOPIC_ARN")),
	}
	resp, err := svc.Publish(params)
	return resp, err
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
