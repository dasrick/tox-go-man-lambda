package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"log"
)

func main() {
	iamlocal := false

	if !iamlocal {
		lambda.Start(HandleRequest)
	} else {

		//	{
		//		"Records": [
		//	{
		//		"EventSource": "aws:sns",
		//		"EventVersion": "1.0",
		//		"EventSubscriptionArn": "arn:aws:sns:eu-west-1:{{accountId}}:ExampleTopic",
		//		"Sns": {
		//			"Type": "Notification",
		//			"MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
		//			"TopicArn": "arn:aws:sns:eu-west-1:123456789012:ExampleTopic",
		//			"Subject": "example subject",
		//			"Message": "example message",
		//			"Timestamp": "1970-01-01T00:00:00.000Z",
		//			"SignatureVersion": "1",
		//			"Signature": "EXAMPLE",
		//			"SigningCertUrl": "EXAMPLE",
		//			"UnsubscribeUrl": "EXAMPLE",
		//			"MessageAttributes": {
		//				"Test": {
		//					"Type": "String",
		//					"Value": "TestString"
		//				},
		//				"TestBinary": {
		//					"Type": "Binary",
		//					"Value": "TestBinary"
		//				}
		//			}
		//		}
		//	}
		//]
		//	}

		testRecord := events.SNSEventRecord{
			EventVersion: "1.0",
			EventSource:  "aws:sns",
			SNS: events.SNSEntity{
				Type: "Notification",
				//Message: {"HaID":"010530410005100006","StID":"0105304100051","HaAdrNStrasse":"Rehhagen","HaAdrNHausnr":"6","HaAdrNHausnrZu":"","HaAdrNPLZ":"23627","HaAdrNOTLName":"","HaAdrNOrtsname":"Groß Grönau","HaKooKGenau":"Adresspunkt"},
			},
		}
		err := HandleRequest(events.SNSEvent{Records: []events.SNSEventRecord{testRecord}})
		if err != nil {
			log.Panicf("ein lustiger fehler:\n %s\n", err)
		}
	}
}

func HandleRequest(snsEvent events.SNSEvent) error {
	for _, snsRecord := range snsEvent.Records {
		log.Println("TheRecord:", snsRecord)
		log.Println("Subject:", snsRecord.SNS.Subject)
		log.Println("Message:", snsRecord.SNS.Message)
		//err := processS3Record(s3record)
		//if err != nil {
		//	return fmt.Errorf("error while processing %s from S3: %s\n", s3record.S3.Object.Key, err)
		//}
	}
	return nil
}
