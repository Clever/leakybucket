package dynamodb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Clever/leakybucket/test"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/stretchr/testify/require"
)

func testRequiredEnv(t *testing.T, key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		t.Logf("required test env var %s not set", key)
		t.FailNow()
	}

	return val
}

func testStorage(t *testing.T) *Storage {
	table := "test-table"

	// Create custom config for testing
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: testRequiredEnv(t, "AWS_DYNAMO_ENDPOINT"),
		}, nil
	})

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "id", SecretAccessKey: "secret", SessionToken: "token",
			},
		}),
	)
	require.NoError(t, err)

	ddb := dynamodb.NewFromConfig(cfg)
	db := bucketDB{
		ddb:       ddb,
		tableName: table,
	}
	// ensure we're working with a clean table
	deleteTable(db)
	err = createTable(db)
	require.NoError(t, err)
	storage, err := New(table, cfg, 10*time.Second)
	require.NoError(t, err)

	return storage
}

func TestCreate(t *testing.T) {
	test.CreateTest(testStorage(t))(t)
}

func TestAdd(t *testing.T) {
	test.AddTest(testStorage(t))(t)
}

func TestThreadSafeAdd(t *testing.T) {
	test.ThreadSafeAddTest(testStorage(t))(t)
}

func TestReset(t *testing.T) {
	test.AddResetTest(testStorage(t))(t)
}

func TestFindOrCreate(t *testing.T) {
	test.FindOrCreateTest(testStorage(t))(t)
}

func TestBucketInstanceConsistencyTest(t *testing.T) {
	test.BucketInstanceConsistencyTest(testStorage(t))(t)
}

// package specific tests
func TestNoTable(t *testing.T) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: os.Getenv("AWS_DYNAMO_ENDPOINT"),
		}, nil
	})

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithEndpointResolverWithOptions(customResolver),
	)
	require.NoError(t, err)
	_, err = New("doesntmatter", cfg, 10*time.Second)
	require.Error(t, err)
}

// TestBucketTTL makes sure the TTL field is being set correctly for dynamodb. The tricky part is
// the actual deletion of the bucket is non-deterministic so there are two success modes:
// - the bucket has been deleted -> we should get an `errBucketNotFound`
// - the bucket has not been deleted -> the TTL field should be set to a time before now
func TestBucketTTL(t *testing.T) {
	s := testStorage(t)
	s.db.ttl = time.Second

	ctx := context.Background()
	_, err := s.db.ddb.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(s.db.tableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("_ttl"),
			Enabled:       aws.Bool(true),
		},
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	bucket, err := s.Create("testbucket", 5, time.Second)
	require.NoError(t, err)

	time.Sleep(s.db.ttl + 10*time.Second)
	dbBucket, err := s.db.bucket("testbucket")
	if err == nil {
		t.Log("bucket not yet deleted. TTL: ", dbBucket.TTL)
		require.NotNil(t, dbBucket)
		require.True(t, dbBucket.TTL.Before(time.Now()))
	} else {
		t.Log("bucket deleted")
		require.Equal(t, errBucketNotFound, err)
	}

	_, err = bucket.Add(1)
	require.NoError(t, err)
}
