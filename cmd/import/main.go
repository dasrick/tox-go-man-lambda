package main

import (
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"io"
	"log"
	"os"
)

var (
	// TOKEN = os.Getenv("TOKEN")
	svc s3iface.S3API
)

func main() {
	svc = s3iface.S3API(s3.New(session.Must(session.NewSession())))
	lambda.Start(HandleRequest)
	//HandleRequest()
}

func HandleRequest(s3Event events.S3Event) error {
	pathIncoming := os.Getenv("S3_PATH_INCOMING")
	log.Println(pathIncoming)
	pathUncompressed := os.Getenv("S3_PATH_UNCOMPRESSED")
	log.Println(pathUncompressed)
	for _, rec := range s3Event.Records {
		//s3 := record.S3
		key := rec.S3.Object.Key
		log.Println(key)

		//dir, file := path.Split(key)
		// Download the file from S3
		obj, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(rec.S3.Bucket.Name),
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("error in downloading %s from S3: %s\n", key, err)
		}

		//body, err := ioutil.ReadAll(obj.Body)
		//if err != nil {
		//	return fmt.Errorf("error in reading file %s: %s\n", key, err)
		//}

		//reader := csv.NewReader(bytes.NewBuffer(body))
		//row, err := reader.ReadAll()
		//if err != nil {
		//	fmt.Println("Error", err)
		//}

		//inputContent, _ := os.Open(InputFile)
		//r := csv.NewReader(bufio.NewReader(body))
		//r := csv.NewReader(bytes.NewBuffer(body))
		r := csv.NewReader(obj.Body)
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
			//genPDF(generatePersonByRecord(record))
			log.Println(record[0])

			lineCount += 1
		}

	}
	return nil
}
