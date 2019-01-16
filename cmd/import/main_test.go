//package main
//
//import (
//	"github.com/aws/aws-lambda-go/events"
//
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//)
//
//var _ = Describe("Import", func() {
//	var (
//		//response events.APIGatewayProxyResponse
//		request events.S3Event
//		err     error
//	)
//
//	JustBeforeEach(func() {
//		//response, err = HandleRequest(request)
//		err = HandleRequest(request)
//		Expect(err).To(BeNil())
//	})
//
//	It(`Returns "Hello, World!" and an OK status`, func() {
//		//Expect(response.Body).To(Equal("Hello, World!"))
//		//Expect(response.StatusCode).To(Equal(http.StatusOK))
//	})
//})
