package dynamodb

import (
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type dbKey string

type db interface {
	createBucket(bucket ddbBucket) error
	bucket(pk dbKey) (*ddbBucket, error)
	incrementBucketValue(pk dbKey, amount, capacity uint) (*ddbBucket, error)
	flushBucket(bucket ddbBucket, expiration time.Time) (*ddbBucket, error)
}

var (
	errConcurrentFlush        = errors.New("concurrent flush operation")
	errBucketCapacityExceeded = errors.New("bucket capacity exceeded")
	errBucketNotFound         = errors.New("bucket not found")
)

var _ db = &bucketDB{}

type bucketDB struct {
	ddb       dynamodbiface.DynamoDBAPI
	tableName string
}

type ddbBucketStatePrimaryKey struct {
	Name string `dynamodbav:"name"`
}

func (d ddbBucketStatePrimaryKey) AttributeDefinitions() []*dynamodb.AttributeDefinition {
	return []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String("name"),
			AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
		},
	}
}

func (d ddbBucketStatePrimaryKey) KeySchema() []*dynamodb.KeySchemaElement {
	return []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String("name"),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	}
}

// ddbBucket implements the db interface using dynamodb as the backend
type ddbBucket struct {
	ddbBucketStatePrimaryKey
	Value uint `dynamodbav:"value"`
	// Expiration indicates when the current rate limit expires. We opt not to use DyanamoDB TTLs
	// because they don't have strong deletion guarantees.
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html
	// "DynamoDB typically deletes expired items within 48 hours of expiration. The exact duration within
	// which an item truly gets deleted after expiration is specific to the nature of the workload
	// and the size of the table."
	Expiration time.Time `dynamodbav:"expiration,unixtime"`
	Version    uint      `dynamodbav:"version"`
}

func newDDBBucket(name string, expiration time.Time) ddbBucket {
	return ddbBucket{
		ddbBucketStatePrimaryKey: ddbBucketStatePrimaryKey{
			Name: name,
		},
		Value:      0,
		Expiration: expiration,
		Version:    0,
	}
}

func decodeBucket(b map[string]*dynamodb.AttributeValue) (*ddbBucket, error) {
	var bs ddbBucket
	if err := dynamodbattribute.UnmarshalMap(b, &bs); err != nil {
		return nil, err
	}
	return &bs, nil
}

func encodeBucket(b ddbBucket) (map[string]*dynamodb.AttributeValue, error) {
	return dynamodbattribute.MarshalMap(b)

}

func (b *ddbBucket) isExpired() bool {
	return time.Now().After(b.Expiration)
}

// _createTable is a method used for testing. For production workloads dynamodb tables
// should be managed by the end user
func (db *bucketDB) _createTable() error {
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
func (db *bucketDB) _deleteTable() error {
	if _, err := db.ddb.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(db.tableName),
	}); err != nil {
		return err
	}
	return db.ddb.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName),
	})
}

func (db *bucketDB) key(pk dbKey) (map[string]*dynamodb.AttributeValue, error) {
	return dynamodbattribute.MarshalMap(ddbBucketStatePrimaryKey{
		Name: string(pk),
	})
}

func (db *bucketDB) bucket(pk dbKey) (*ddbBucket, error) {
	key, err := db.key(pk)
	if err != nil {
		return nil, err
	}
	res, err := db.ddb.GetItem(&dynamodb.GetItemInput{
		Key:            key,
		TableName:      aws.String(db.tableName),
		ConsistentRead: aws.Bool(true),
	})
	if awsErr(err, dynamodb.ErrCodeResourceNotFoundException) || len(res.Item) == 0 {
		return nil, errBucketNotFound
	} else if err != nil {
		return nil, err
	}

	return decodeBucket(res.Item)
}

func (db *bucketDB) createBucket(bucket ddbBucket) error {
	data, err := encodeBucket(bucket)
	if err != nil {
		return err
	}
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
	_, err = db.ddb.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(db.tableName),
		Item:      data,
		ExpressionAttributeNames: map[string]*string{
			"#N": aws.String("name"),
		},
		ConditionExpression: aws.String("attribute_not_exists(#N)"),
	})

	return err
}

func (db *bucketDB) incrementBucketValue(pk dbKey, amount, capacity uint) (*ddbBucket, error) {
	key, err := db.key(pk)
	if err != nil {
		return nil, err
	}
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.SET.IncrementAndDecrement
	res, err := db.ddb.UpdateItem(&dynamodb.UpdateItemInput{
		Key:       key,
		TableName: aws.String(db.tableName),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":a": {
				N: aws.String(strconv.FormatUint(uint64(amount), 10)),
			},
			":c": {
				N: aws.String(strconv.FormatUint(uint64(capacity), 10)),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#V": aws.String("value"),
		},
		ReturnValues:        aws.String(dynamodb.ReturnValueAllNew),
		UpdateExpression:    aws.String("SET #V = #V + :a"),
		ConditionExpression: aws.String("#V <= :c"),
	})
	if err != nil {
		if awsErr(err, dynamodb.ErrCodeConditionalCheckFailedException) {
			return nil, errBucketCapacityExceeded
		}
		return nil, err
	}
	return decodeBucket(res.Attributes)

}

// flushBucket will reset the bucket's value to 0 iff the versions match
func (db *bucketDB) flushBucket(bucket ddbBucket, expiration time.Time) (*ddbBucket, error) {
	// dbMaxVersion is an arbitrary constant to prevent the version field from overflowing
	var dbMaxVersion uint = 2 << 28
	newVersion := bucket.Version + 1
	if newVersion > dbMaxVersion {
		newVersion = 0
	}

	updatedBucket := ddbBucket{
		ddbBucketStatePrimaryKey: bucket.ddbBucketStatePrimaryKey,
		Value:                    0,
		Expiration:               expiration,
		Version:                  newVersion,
	}
	data, err := encodeBucket(updatedBucket)
	if err != nil {
		return nil, err
	}
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html#Expressions.ConditionExpressions.SimpleComparisons
	_, err = db.ddb.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(db.tableName),
		Item:      data,
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": {
				N: aws.String(strconv.FormatUint(uint64(bucket.Version), 10)),
			},
		},
		ConditionExpression: aws.String("version = :v"),
	})
	if err != nil {
		if awsErr(err, dynamodb.ErrCodeConditionalCheckFailedException) {
			return nil, errConcurrentFlush
		}
		return nil, err
	}
	return &updatedBucket, nil
}

func awsErr(err error, codes ...string) bool {
	if err == nil {
		return false
	}
	if aerr, ok := err.(awserr.Error); ok {
		for _, code := range codes {
			if code == aerr.Code() {
				return true
			}
		}
	}
	return false
}
