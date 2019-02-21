package snsclient

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

type Client struct {
	svc *sns.SNS
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
	svc := sns.New(sess)
	// map to client
	result.svc = svc
	// thats it
	return result, nil
}

func (c Client) Publish(message string, topicArn string) (*sns.PublishOutput, error) {
	return c.svc.Publish(&sns.PublishInput{
		Message:  aws.String(message),
		TopicArn: aws.String(topicArn),
	})
}
