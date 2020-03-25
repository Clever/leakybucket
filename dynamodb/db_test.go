package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// _createTable is a method used for testing. For production workloads dynamodb tables
// should be managed by the end user
func createTable(db *bucketDB) error {
	input := &dynamodb.CreateTableInput{
		TableName:            aws.String(db.tableName),
		BillingMode:          aws.String(dynamodb.BillingModePayPerRequest),
		AttributeDefinitions: ddbBucketStatePrimaryKey{}.AttributeDefinitions(),
		KeySchema:            ddbBucketStatePrimaryKey{}.KeySchema(),
	}
	if _, err := db.ddb.CreateTable(input); err != nil {
		return err
	}
	return db.ddb.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	})
}

// _deleteTable is a method used for testing
func deleteTable(db *bucketDB) error {
	if _, err := db.ddb.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(db.tableName),
	}); err != nil {
		return err
	}
	return db.ddb.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	})
}
