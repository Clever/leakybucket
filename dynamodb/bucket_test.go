package dynamodb

import (
	"os"
	"testing"

	"github.com/Clever/leakybucket/test"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

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
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String("doesntmatter"),
		Endpoint:    aws.String(testRequiredEnv(t, "AWS_DYNAMO_ENDPOINT")),
		Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
	})
	ddb := dynamodb.New(s)
	db := bucketDB{
		ddb:       ddb,
		tableName: table,
	}
	// ensure we're working with a clean table
	deleteTable(db)
	err = createTable(db)
	require.NoError(t, err)
	storage, err := New(table, s)
	require.NoError(t, err)

	return storage
}

func TestNoTable(t *testing.T) {
	session, err := session.NewSession(&aws.Config{
		Endpoint: aws.String(os.Getenv("AWS_DYNAMO_ENDPOINT")),
	})
	require.NoError(t, err)
	_, err = New("doesntmatter", session)
	require.Error(t, err)
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
