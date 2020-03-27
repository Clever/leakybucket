package dynamodb

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	errBucketCapacityExceeded = errors.New("bucket capacity exceeded")
	errBucketNotFound         = errors.New("bucket not found")
)

type bucketDB struct {
	ddb       dynamodbiface.DynamoDBAPI
	tableName string
	ttl       time.Duration
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
	// Expiration indicates when the current rate limit expires. We opt not to use DyanamoDB TTLs
	// because they don't have strong deletion guarantees.
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html
	// "DynamoDB typically deletes expired items within 48 hours of expiration. The exact duration within
	// which an item truly gets deleted after expiration is specific to the nature of the workload
	// and the size of the table."
	Expiration time.Time `dynamodbav:"expiration,unixtime"`
	// Value is the sum of all increments in the current sliding window for the bucket
	Value uint `dynamodbav:"value"`
	// Version is an internal field used to control flushing/draining the Value field concurrently
	Version uint `dynamodbav:"version"`
	// TTL is an internal attribute to define how long the item will live in dynamodb prior to being
	// set for removal. This TTL mechanism is only used for good hygiene to ensure we don't leave
	// unused buckets in the database forever
	TTL time.Time `dynamodbav:"_ttl,unixtime"`
}

func newDDBBucket(name string, expiresIn time.Duration, ttl time.Duration) ddbBucket {
	now := time.Now()
	return ddbBucket{
		ddbBucketStatePrimaryKey: ddbBucketStatePrimaryKey{
			Name: name,
		},
		Expiration: now.Add(expiresIn),
		Value:      0,
		Version:    0,
		TTL:        now.Add(ttl),
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

func (b *ddbBucket) expired() bool {
	return time.Now().After(b.Expiration)
}

func (db bucketDB) key(name string) (map[string]*dynamodb.AttributeValue, error) {
	return dynamodbattribute.MarshalMap(ddbBucketStatePrimaryKey{
		Name: string(name),
	})
}

func (db bucketDB) bucket(name string) (*ddbBucket, error) {
	key, err := db.key(name)
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

func (db bucketDB) findOrCreateBucket(name string, expiresIn time.Duration) (*ddbBucket, error) {
	dbBucket, err := db.bucket(name)
	if err == nil {
		return dbBucket, nil
	} else if err != errBucketNotFound {
		return nil, err
	}

	// otherwise create the bucket
	bucket := newDDBBucket(name, expiresIn, db.ttl)
	data, err := encodeBucket(bucket)
	if err != nil {
		return nil, err
	}
	_, err = db.ddb.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(db.tableName),
		Item:      data,
		ExpressionAttributeNames: map[string]*string{
			"#N": aws.String("name"),
		},
		ConditionExpression: aws.String("attribute_not_exists(#N)"),
	})
	if err != nil {
		if !awsErr(err, dynamodb.ErrCodeConditionalCheckFailedException) {
			return nil, err
		}
		// insane edge case because we know we can have multiple consumers
		// for existing buckets simply re-fetch
		return db.bucket(bucket.Name)
	}

	return &bucket, err
}

func (db bucketDB) incrementBucketValue(name string, amount, capacity uint) (*ddbBucket, error) {
	key, err := db.key(name)
	if err != nil {
		return nil, err
	}
	res, err := db.ddb.UpdateItem(&dynamodb.UpdateItemInput{
		Key:       key,
		TableName: aws.String(db.tableName),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":a": {
				N: aws.String(fmt.Sprintf("%d", amount)),
			},
			":c": {
				N: aws.String(fmt.Sprintf("%d", capacity)),
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

// resetBucket will reset the bucket's value to 0 iff the versions match
func (db bucketDB) resetBucket(bucket ddbBucket, expiresIn time.Duration) (*ddbBucket, error) {
	// dbMaxVersion is an arbitrary constant to prevent the version field from overflowing
	var dbMaxVersion uint = 2 << 28
	newVersion := bucket.Version + 1
	if newVersion > dbMaxVersion {
		newVersion = 0
	}
	updatedBucket := newDDBBucket(bucket.ddbBucketStatePrimaryKey.Name, expiresIn, db.ttl)
	updatedBucket.Version = newVersion
	data, err := encodeBucket(updatedBucket)
	if err != nil {
		return nil, err
	}
	_, err = db.ddb.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(db.tableName),
		Item:      data,
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": {
				N: aws.String(fmt.Sprintf("%0d", bucket.Version)),
			},
		},
		ConditionExpression: aws.String("version = :v"),
	})
	if err != nil {
		if !awsErr(err, dynamodb.ErrCodeConditionalCheckFailedException) {
			return nil, err
		}
		// A conditional check failing means another consumer of this bucket reset at the same time.
		// We can simply swallow the error and re-fetch the bucket
		return db.bucket(bucket.Name)
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
