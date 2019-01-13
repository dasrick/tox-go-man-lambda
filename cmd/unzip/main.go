package main

import (
	"log"
)

func main() {
	//lambda.Start(HandleRequest)
	HandleRequest()
}

//func HandleRequest(s3Event events.S3Event) () {
//	for _, record := range s3Event.Records {
//		s3 := record.S3
//		fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, s3.Bucket.Name, s3.Object.Key)
//	}
//}

func HandleRequest() {
	//for _, record := range s3Event.Records {
	//	s3 := record.S3
	//	fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, s3.Bucket.Name, s3.Object.Key)
	//}

	log.Print("hui buh")
}
