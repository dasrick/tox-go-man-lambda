package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"strings"
)

type IncomingObject struct {
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

//type SourceItemData struct {
//	HaID           string
//	StID           string
//	HaAdrNStrasse  string
//	HaAdrNHausnr   string
//	HaAdrNHausnrZu string
//	HaAdrNPLZ      string
//	HaAdrNOTLName  string
//	HaAdrNOrtsname string
//	HaKooKGenau    string
//}
//
//type SourceItem struct {
//	HaID    string
//	DataSet SourceItemData
//}

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
	incomingObject, err := getIncomingObjectFromString(snsRecord.SNS.Message)
	if err != nil {
		return err
	}

	err = processSource(incomingObject)
	if err != nil {
		return err
	}

	return nil
}

// main steps
func processSource(incomingObject *IncomingObject) error {
	svc := getS3Service("eu-west-1")

	prefixSource := "data/source/"            // ToDo should be part of a configuration or similar
	bucket := "man-deveh-stash"               // ToDo check to variable
	key := prefixSource + incomingObject.HaID // ToDo does exists someting like join(arrayOfString, glue)

	jsonContent, err := json.MarshalIndent(incomingObject, "", "\t")

	// check if a record exists
	exists, err := existObject(svc, bucket, key)
	if err != nil {
		return err
	}
	if exists == true {
		// if exists - update (replace or append? => here it is REPLACE)
		//log.Println("it's time to UPDATE the existing file")
		_, err := replaceObject(svc, bucket, key, jsonContent)
		if err != nil {
			return err
		}
		//log.Println(result)
	} else {
		// if NOT exists - insert
		//log.Println("it's time to CREATE a new file")
		_, err := putObject(svc, bucket, key, jsonContent)
		if err != nil {
			return err
		}
		//log.Println(result)
	}

	return nil
}

// substeps

// helper
func getIncomingObjectFromString(jsonString string) (*IncomingObject, error) {
	incomingObject := &IncomingObject{}
	err := json.Unmarshal([]byte(jsonString), incomingObject)
	if err != nil {
		log.Printf("error unmarshal %s from SNS: %s\n", jsonString, err)
	}
	return incomingObject, err
}

// s3 helper
func getS3Service(region string) *s3.S3 {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	svc := s3.New(sess)
	return svc
}

func headObject(svc *s3.S3, bucket string, key string) (*s3.HeadObjectOutput, error) {
	obj, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return obj, err
}

func getObject(svc *s3.S3, bucket string, key string) (*s3.GetObjectOutput, error) {
	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return obj, err
}

func putObject(svc *s3.S3, bucket string, key string, content []byte) (*s3.PutObjectOutput, error) {
	obj, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(content)),
	})
	return obj, err
}

func deleteObject(svc *s3.S3, bucket string, key string) error {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	return nil
}

func existObject(svc *s3.S3, bucket string, key string) (bool, error) {
	exists := true
	_, err := headObject(svc, bucket, key)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
			case "NotFound": // because of a bug in go sdk
				exists = false
				err = nil
				break
			default:
				//fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			//fmt.Println(err.Error())
		}
		//return

		//fmt.Println(err)
	}
	return exists, err
}

func replaceObject(svc *s3.S3, bucket string, key string, content []byte) (*s3.PutObjectOutput, error) {
	// delete current existing object
	err := deleteObject(svc, bucket, key)
	if err != nil {
		return nil, err
	}
	// create a new one
	result, err := putObject(svc, bucket, key, content)
	return result, err
}

func appendObject(svc *s3.S3, bucket string, key string, content []byte) {
	// get object (content - should be an JSON array)
	// build new content
	// call replace with new content
}

//func getDynamoDBService(region string) *dynamodb.DynamoDB {
//	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
//	svc := dynamodb.New(sess)
//	return svc
//}
//
//func processDDBSource(rowObject *RowObject) error {
//	var err error
//	// 1st - select by key (try to find existing entry)
//	resultQuery, err := querySource(rowObject)
//	if err != nil {
//		//return fmt.Errorf("error while query in source")
//		return err
//	}
//	// 2nd - if hit then update else insert
//	if int64(*resultQuery.Count) == 0 { // hard compare ... for me ... there are better ways ...
//		resultInsert, err := putItemSource(rowObject)
//		if err != nil {
//			//return fmt.Errorf("error while insert in source")
//			return err
//		}
//		log.Println(resultInsert) // ToDo check ... maybe not needed
//	} else {
//		resultUpdate, err := updateItemSource(rowObject)
//		if err != nil {
//			//return fmt.Errorf("error while update in source")
//			return err
//		}
//		log.Println(resultUpdate) // ToDo check ... maybe not needed
//	}
//
//	return err
//}
//
//func querySource(rowObject *RowObject) (*dynamodb.QueryOutput, error) {
//	svc := getDynamoDBService("eu-west-1")         // ToDo region should be variable
//	tableName := os.Getenv("DYNAMODB_SOURCE_NAME") // ToDo enable live vars ... or move into main
//	//tableName := "man-deveh-source"
//	input := &dynamodb.QueryInput{
//		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
//			":v1": {
//				S: aws.String(rowObject.HaID),
//			},
//		},
//		KeyConditionExpression: aws.String("HaID = :v1"),
//		TableName:              aws.String(tableName),
//	}
//	result, err := svc.Query(input)
//	return result, err
//}
//
//func putItemSource(rowObject *RowObject) (*dynamodb.PutItemOutput, error) {
//	svc := getDynamoDBService("eu-west-1")         // ToDo region should be variable
//	tableName := os.Getenv("DYNAMODB_SOURCE_NAME") // ToDo enable live vars ... or move into main
//	//tableName := "man-deveh-source"
//
//	sourceItem := getSourceItem(rowObject)
//	si, err := dynamodbattribute.MarshalMap(sourceItem)
//
//	input := &dynamodb.PutItemInput{
//		Item: si,
//		//ReturnConsumedCapacity: aws.String("TOTAL"),
//		TableName: aws.String(tableName),
//	}
//	result, err := svc.PutItem(input)
//	// deteiled error
//	if err != nil {
//		if aerr, ok := err.(awserr.Error); ok {
//			switch aerr.Code() {
//			case dynamodb.ErrCodeConditionalCheckFailedException:
//				fmt.Println(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
//			case dynamodb.ErrCodeProvisionedThroughputExceededException:
//				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
//			case dynamodb.ErrCodeResourceNotFoundException:
//				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
//			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
//				fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
//			case dynamodb.ErrCodeTransactionConflictException:
//				fmt.Println(dynamodb.ErrCodeTransactionConflictException, aerr.Error())
//			case dynamodb.ErrCodeRequestLimitExceeded:
//				fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
//			case dynamodb.ErrCodeInternalServerError:
//				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
//			default:
//				fmt.Println(aerr.Error())
//			}
//		} else {
//			// Print the error, cast err to awserr.Error to get the Code and
//			// Message from an error.
//			fmt.Println(err.Error())
//		}
//		//return
//	}
//	// deteiled error
//	return result, err
//}
//
//func updateItemSource(rowObject *RowObject) (*dynamodb.UpdateItemOutput, error) {
//	svc := getDynamoDBService("eu-west-1")         // ToDo region should be variable
//	tableName := os.Getenv("DYNAMODB_SOURCE_NAME") // ToDo enable live vars ... or move into main
//	//tableName := "man-deveh-source"
//
//	sourceItemData := getSourceItemData(rowObject)
//	sid, err := dynamodbattribute.MarshalMap(sourceItemData)
//
//	input := &dynamodb.UpdateItemInput{
//		Key:                       map[string]*dynamodb.AttributeValue{"HaID": {S: aws.String(rowObject.HaID)}},
//		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":m": {M: sid}},
//		UpdateExpression:          aws.String("SET DataSet = :m"),
//		//ReturnValues:     aws.String("ALL_NEW"),
//		TableName: aws.String(tableName),
//	}
//
//	result, err := svc.UpdateItem(input)
//
//	// deteiled error
//	if err != nil {
//		if aerr, ok := err.(awserr.Error); ok {
//			switch aerr.Code() {
//			case dynamodb.ErrCodeConditionalCheckFailedException:
//				fmt.Println(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
//			case dynamodb.ErrCodeProvisionedThroughputExceededException:
//				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
//			case dynamodb.ErrCodeResourceNotFoundException:
//				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
//			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
//				fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
//			case dynamodb.ErrCodeTransactionConflictException:
//				fmt.Println(dynamodb.ErrCodeTransactionConflictException, aerr.Error())
//			case dynamodb.ErrCodeRequestLimitExceeded:
//				fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
//			case dynamodb.ErrCodeInternalServerError:
//				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
//			default:
//				fmt.Println(aerr.Error())
//			}
//		} else {
//			// Print the error, cast err to awserr.Error to get the Code and
//			// Message from an error.
//			fmt.Println(err.Error())
//		}
//	}
//	// deteiled error
//
//	return result, err
//}
//
//func getSourceItem(rowObject *RowObject) SourceItem {
//	sourceItem := SourceItem{
//		HaID:    rowObject.HaID,
//		DataSet: getSourceItemData(rowObject),
//	}
//	return sourceItem
//}
//
//func getSourceItemData(rowObject *RowObject) SourceItemData {
//	sourceItemData := SourceItemData{
//		HaID:           rowObject.HaID,
//		StID:           rowObject.StID,
//		HaAdrNStrasse:  rowObject.HaAdrNStrasse,
//		HaAdrNHausnr:   rowObject.HaAdrNHausnr,
//		HaAdrNHausnrZu: rowObject.HaAdrNHausnrZu,
//		HaAdrNPLZ:      rowObject.HaAdrNPLZ,
//		HaAdrNOTLName:  rowObject.HaAdrNOTLName,
//		HaAdrNOrtsname: rowObject.HaAdrNOrtsname,
//		HaKooKGenau:    rowObject.HaKooKGenau,
//	}
//	return sourceItemData
//}
