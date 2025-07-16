package dynamodb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func createTable(db bucketDB) error {
	input := &dynamodb.CreateTableInput{
		TableName:            aws.String(db.tableName),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: ddbBucketStatePrimaryKey{}.AttributeDefinitions(),
		KeySchema:            ddbBucketStatePrimaryKey{}.KeySchema(),
	}
	if _, err := db.ddb.CreateTable(context.TODO(), input); err != nil {
		return err
	}

	// Wait for table to exist
	waiter := dynamodb.NewTableExistsWaiter(db.ddb)
	return waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	}, 30*time.Second)
}

func deleteTable(db bucketDB) error {
	if _, err := db.ddb.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: aws.String(db.tableName),
	}); err != nil {
		return err
	}

	// Wait for table to not exist
	waiter := dynamodb.NewTableNotExistsWaiter(db.ddb)
	return waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	}, 30*time.Second)
}
