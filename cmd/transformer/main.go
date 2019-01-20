package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"log"
	"os"
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

type SourceItemData struct {
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

type SourceItem struct {
	HaID    string
	DataSet SourceItemData
}

func main() {
	iamlocal := false

	if !iamlocal {
		lambda.Start(HandleRequest)
	} else {
		testRecord := events.SNSEventRecord{
			EventVersion: "1.0",
			EventSource:  "aws:sns",
			SNS: events.SNSEntity{
				Type:    "Notification",
				Message: `{"HaID":"010530410005100006","StID":"0105304100051","HaAdrNStrasse":"Rehhagen","HaAdrNHausnr":"6","HaAdrNHausnrZu":"","HaAdrNPLZ":"23627","HaAdrNOTLName":"","HaAdrNOrtsname":"Groß Grönau","HaKooKGenau":"Adresspunkt"}`,
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
		err := processSNSRecord(snsRecord)
		if err != nil {
			return fmt.Errorf("error while processing %s from SNS: %s\n", snsRecord, err)
		}
	}
	return nil
}

func processSNSRecord(snsRecord events.SNSEventRecord) error {
	// serialize JSON string (Message)
	rowObject := &RowObject{}
	err := json.Unmarshal([]byte(snsRecord.SNS.Message), rowObject)
	if err != nil {
		return fmt.Errorf("error unmarshal %s from SNS: %s\n", snsRecord.SNS.Message, err)
	}

	// and now go for gold ...
	err = processDDBSource(rowObject)
	if err != nil {
		return fmt.Errorf("error processDDBSource %s from rowObject: %s\n", rowObject, err)
	}

	return nil
}

func getDynamoDBService(region string) *dynamodb.DynamoDB {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	svc := dynamodb.New(sess)
	return svc
}

func processDDBSource(rowObject *RowObject) error {
	var err error
	// 1st - select by key (try to find existing entry)
	resultQuery, err := querySource(rowObject)
	if err != nil {
		//return fmt.Errorf("error while query in source")
		return err
	}
	// 2nd - if hit then update else insert
	if int64(*resultQuery.Count) == 0 { // hard compare ... for me ... there are better ways ...
		resultInsert, err := putItemSource(rowObject)
		if err != nil {
			//return fmt.Errorf("error while insert in source")
			return err
		}
		log.Println(resultInsert) // ToDo check ... maybe not needed
	} else {
		resultUpdate, err := updateItemSource(rowObject)
		if err != nil {
			//return fmt.Errorf("error while update in source")
			return err
		}
		log.Println(resultUpdate) // ToDo check ... maybe not needed
	}

	return err
}

func querySource(rowObject *RowObject) (*dynamodb.QueryOutput, error) {
	svc := getDynamoDBService("eu-west-1")         // ToDo region should be variable
	tableName := os.Getenv("DYNAMODB_SOURCE_NAME") // ToDo enable live vars ... or move into main
	//tableName := "man-deveh-source"
	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				S: aws.String(rowObject.HaID),
			},
		},
		KeyConditionExpression: aws.String("HaID = :v1"),
		TableName:              aws.String(tableName),
	}
	result, err := svc.Query(input)
	return result, err
}

func putItemSource(rowObject *RowObject) (*dynamodb.PutItemOutput, error) {
	svc := getDynamoDBService("eu-west-1")         // ToDo region should be variable
	tableName := os.Getenv("DYNAMODB_SOURCE_NAME") // ToDo enable live vars ... or move into main
	//tableName := "man-deveh-source"

	sourceItem := getSourceItem(rowObject)
	si, err := dynamodbattribute.MarshalMap(sourceItem)

	input := &dynamodb.PutItemInput{
		Item: si,
		//ReturnConsumedCapacity: aws.String("TOTAL"),
		TableName: aws.String(tableName),
	}
	result, err := svc.PutItem(input)
	// deteiled error
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				fmt.Println(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeTransactionConflictException:
				fmt.Println(dynamodb.ErrCodeTransactionConflictException, aerr.Error())
			case dynamodb.ErrCodeRequestLimitExceeded:
				fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		//return
	}
	// deteiled error
	return result, err
}

func updateItemSource(rowObject *RowObject) (*dynamodb.UpdateItemOutput, error) {
	svc := getDynamoDBService("eu-west-1")         // ToDo region should be variable
	tableName := os.Getenv("DYNAMODB_SOURCE_NAME") // ToDo enable live vars ... or move into main
	//tableName := "man-deveh-source"

	sourceItemData := getSourceItemData(rowObject)
	sid, err := dynamodbattribute.MarshalMap(sourceItemData)

	input := &dynamodb.UpdateItemInput{
		Key:                       map[string]*dynamodb.AttributeValue{"HaID": {S: aws.String(rowObject.HaID)}},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":m": {M: sid}},
		UpdateExpression:          aws.String("SET DataSet = :m"),
		//ReturnValues:     aws.String("ALL_NEW"),
		TableName: aws.String(tableName),
	}

	result, err := svc.UpdateItem(input)

	// deteiled error
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				fmt.Println(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeTransactionConflictException:
				fmt.Println(dynamodb.ErrCodeTransactionConflictException, aerr.Error())
			case dynamodb.ErrCodeRequestLimitExceeded:
				fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}
	// deteiled error

	return result, err
}

func getSourceItem(rowObject *RowObject) SourceItem {
	sourceItem := SourceItem{
		HaID:    rowObject.HaID,
		DataSet: getSourceItemData(rowObject),
	}
	return sourceItem
}

func getSourceItemData(rowObject *RowObject) SourceItemData {
	sourceItemData := SourceItemData{
		HaID:           rowObject.HaID,
		StID:           rowObject.StID,
		HaAdrNStrasse:  rowObject.HaAdrNStrasse,
		HaAdrNHausnr:   rowObject.HaAdrNHausnr,
		HaAdrNHausnrZu: rowObject.HaAdrNHausnrZu,
		HaAdrNPLZ:      rowObject.HaAdrNPLZ,
		HaAdrNOTLName:  rowObject.HaAdrNOTLName,
		HaAdrNOrtsname: rowObject.HaAdrNOrtsname,
		HaKooKGenau:    rowObject.HaKooKGenau,
	}
	return sourceItemData
}
