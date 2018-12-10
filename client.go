package dynamodbclient

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// AWS Config
type config struct {
	region                string
	accessKeyID           string
	accessSecretAccessKey string
	dynamodbEndpoint      string
}

// Item
type eventItem struct {
	name []byte
	time time.Time
}

// DynamoDBClient - Client
type DynamoDBClient struct {
	srv    *dynamodb.DynamoDB
	table  string
	config config
}

// Connect - connect to Connect
func (db DynamoDBClient) Connect() {
	dynamodbEndpoint := os.Getenv("DYNAMODB_ENDPOINT")

	config := &aws.Config{
		Region:   aws.String(os.Getenv("AWS_REGION")),
		Endpoint: aws.String(fmt.Sprintf("http://%v:8000", dynamodbEndpoint)),
		Credentials: credentials.NewStaticCredentials(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"123",
		),
	}

	sess := session.Must(session.NewSession(config))

	db.srv = dynamodb.New(sess)

}

// CreateTable - Create a table in Connect
func (db DynamoDBClient) CreateTable() {

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("name"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("name"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String("jjjjjj"),
	}

	resp, err := db.srv.CreateTable(input)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	fmt.Println(resp)
}

// PutItem - put an item into DynamoDB
func (db DynamoDBClient) PutItem(e eventItem) {

	// marshal the movie struct into an aws attribute value
	eventMessage, err := dynamodbattribute.MarshalMap(e)
	if err != nil {
		panic("Cannot marshal movie into AttributeValue map")
	}

	// create the api params
	params := &dynamodb.PutItemInput{
		TableName: aws.String(db.table),
		Item:      eventMessage,
	}

	// put the item
	resp, err := db.srv.PutItem(params)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err.Error())
		return
	}

	log.Printf("Response: %v", resp)
}
