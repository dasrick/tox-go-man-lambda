package s3client

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Client struct {
	svc *s3.S3
}

type Options struct {
	Region string
}

var DefaultOptions = Options{
	Region: "eu-west-1",
}

func NewClient(options Options) (Client, error) {
	result := Client{}
	// check incoming values and use default as fallback
	if options.Region == "" {
		options.Region = DefaultOptions.Region
	}
	// create session
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(options.Region)}))
	svc := s3.New(sess)
	// map to client
	result.svc = svc
	// thats it
	return result, nil
}

func (c Client) GetByRecord(s3record events.S3EventRecord) (*s3.GetObjectOutput, error) {
	return c.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3record.S3.Bucket.Name),
		Key:    aws.String(s3record.S3.Object.Key),
	})
}

func (c Client) DeleteByRecord(s3record events.S3EventRecord) (*s3.DeleteObjectOutput, error) {
	return c.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s3record.S3.Bucket.Name),
		Key:    aws.String(s3record.S3.Object.Key),
	})
}
