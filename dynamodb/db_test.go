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
	ctx := context.Background()
	if _, err := db.ddb.CreateTable(ctx, input); err != nil {
		return err
	}

	// Wait for table to exist
	waiter := dynamodb.NewTableExistsWaiter(db.ddb)
	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	}, 30*time.Second)
}

func deleteTable(db bucketDB) error {
	ctx := context.Background()
	if _, err := db.ddb.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(db.tableName),
	}); err != nil {
		return err
	}

	// Wait for table to not exist
	waiter := dynamodb.NewTableNotExistsWaiter(db.ddb)
	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	}, 30*time.Second)
}
