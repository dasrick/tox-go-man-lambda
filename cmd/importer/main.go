package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"tox-go-man-lambda/internal/pkg/micron"
	"tox-go-man-lambda/internal/pkg/s3client"
	"tox-go-man-lambda/internal/pkg/snsclient"
)

func HandleRequest(s3Event events.S3Event) error {
	for _, s3Record := range s3Event.Records {
		// Fahrplan ====================================================================================================
		log.Printf("START PROCESSING %s\n", s3Record.S3.Object.Key)
		// -------------------------------------------------------------------------------------------------------------
		// s3 client erzeugen
		s3Options := s3client.Options{}
		s3Svc, err := s3client.NewClient(s3Options)
		if err != nil {
			return err
		}
		// sns client erzeugen
		snsOptions := snsclient.Options{}
		snsSvc, err := snsclient.NewClient(snsOptions)
		if err != nil {
			return err
		}
		// -------------------------------------------------------------------------------------------------------------
		// s3object einlesen
		obj, err := s3Svc.GetByRecord(s3Record)
		if err != nil {
			return err
		}
		// -------------------------------------------------------------------------------------------------------------
		// je nach file extension dann zeilenweise einlesen
		// check if it is a *.gz or *.csv
		var inputContent io.Reader
		if filepath.Ext(s3Record.S3.Object.Key) == ".gz" {
			gr, err := gzip.NewReader(obj.Body)
			if err := gr.Close(); err != nil {
				log.Fatal(err)
			}
			if err != nil {
				return err
			}
			inputContent = gr
		} else {
			inputContent = obj.Body
		}
		// build CSV reader
		r := csv.NewReader(bufio.NewReader(inputContent))
		r.Comma = ';'
		r.Comment = '#'
		// -------------------------------------------------------------------------------------------------------------
		// je zeile etwas tun
		lineCount := 0
		for {
			record, err := r.Read()
			// Stop at EOF.
			if err == io.EOF {
				break
			}
			// drop first line because of header rows
			if lineCount <= 0 {
				lineCount++
				continue
			}
			// now its time to do something with this object
			// sub - create object from csv data
			rowObject := generateRowObject(record)
			// sub - create json from object
			rowJSON, err := json.Marshal(rowObject)
			if err != nil {
				return err
			}
			// sub - prepare sns
			msg := string(rowJSON)
			topicArn := os.Getenv("SNS_TOPIC_ARN")
			if topicArn == "" {
				return errors.New("missing SNS_TOPIC_ARN")
			}
			// sub - trigger sns publish
			_, err = snsSvc.Publish(msg, topicArn)
			if err != nil {
				return err
			}
			lineCount++
		}
		// some output for cloudwatch
		log.Printf("PROCESSED LINES %s\n", strconv.Itoa(lineCount))
		log.Printf("FINISH PROCESSING %s\n", s3Record.S3.Object.Key)
		// -------------------------------------------------------------------------------------------------------------
		// nach erfolgreicher abarbeitung s3object löschen
		_, err = s3Svc.DeleteByRecord(s3Record)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	lambda.Start(HandleRequest)
}

func generateRowObject(record []string) micron.CsvObject {
	return micron.CsvObject{
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
}
