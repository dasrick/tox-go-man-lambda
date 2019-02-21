package main

import (
	"aws-helper-go/kvdynamodb"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"os"
	"tox-go-man-lambda/internal/pkg/micron"
)

func HandleRequest(snsEvent events.SNSEvent) error {
	for _, snsRecord := range snsEvent.Records {
		// Fahrplan ====================================================================================================
		// man extrahiere das übermittelte object aus der sns
		incomingObject := &micron.CsvObject{}
		err := json.Unmarshal([]byte(snsRecord.SNS.Message), incomingObject)
		if err != nil {
			return err
		}
		// transformiere es (hier wird es unverändert gespeichert ... wir machen also erst 'mal nix ... original first)

		// dynamodb client erzeugen (je transformation)
		dynamodbOptions := kvdynamodb.DefaultOptions
		dynamodbOptions.Region = "eu-west-1"
		dynamodbOptions.TableName = os.Getenv("DYNAMODB_SOURCE_NAME")
		dynamodbOptions.HashKey = "HaID"
		dynamobdClient, err := kvdynamodb.NewClient(dynamodbOptions)
		if err != nil {
			return err
		}
		// man speichere es in der zur transformation gehörigen ... hmmm ... db
		err = dynamobdClient.Store(incomingObject.HaID, incomingObject)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	lambda.Start(HandleRequest)
}
