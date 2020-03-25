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

func setupTestTable(t *testing.T, table string, s *session.Session) {
	ddb := dynamodb.New(s)
	db := &bucketDB{
		ddb:       ddb,
		tableName: table,
	}
	db._deleteTable()
	err := db._createTable()
	require.NoError(t, err)
}

func getLocalStorage(t *testing.T) *Storage {
	testTable := "test-table"
	session, err := session.NewSession(&aws.Config{
		Region:      aws.String("doesntmatter"),
		Endpoint:    aws.String(os.Getenv("AWS_DYNAMO_ENDPOINT")),
		Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
	})
	require.NoError(t, err)
	setupTestTable(t, testTable, session)
	storage, err := New(testTable, session)
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
	test.CreateTest(getLocalStorage(t))(t)
}

func TestAdd(t *testing.T) {
	test.AddTest(getLocalStorage(t))(t)
}

func TestThreadSafeAdd(t *testing.T) {
	// DynamoDB Add is not thread safe.
	t.Skip()
	test.ThreadSafeAddTest(getLocalStorage(t))(t)
}

func TestReset(t *testing.T) {
	test.AddResetTest(getLocalStorage(t))(t)
}

func TestFindOrCreate(t *testing.T) {
	test.FindOrCreateTest(getLocalStorage(t))(t)
}

func TestBucketInstanceConsistencyTest(t *testing.T) {
	test.BucketInstanceConsistencyTest(getLocalStorage(t))(t)
}
