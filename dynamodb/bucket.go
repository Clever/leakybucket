package dynamodb

import (
	"sync"
	"time"

	"github.com/Clever/leakybucket"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var _ leakybucket.Bucket = &bucket{}

type bucket struct {
	name                string
	capacity, remaining uint
	reset               time.Time
	rate                time.Duration
	db                  bucketDB
	mutex               sync.Mutex
}

// Capacity ...
func (b *bucket) Capacity() uint {
	return b.capacity
}

// Remaining space in the bucket.
func (b *bucket) Remaining() uint {
	return b.remaining
}

// Reset returns when the bucket will be drained.
func (b *bucket) Reset() time.Time {
	return b.reset
}

// Add to the bucket.
func (b *bucket) Add(amount uint) (leakybucket.BucketState, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	// Storage.Create guarantees the DB Bucket with a configured TTL. For long running executions it
	// is possible old buckets will get deleted, so we use `findOrCreate` rather than `bucket`
	dbBucket, err := b.db.findOrCreateBucket(b.name, b.rate)
	if err != nil {
		return b.state(), err
	}
	if dbBucket.expired() {
		dbBucket, err = b.db.resetBucket(*dbBucket, b.rate)
		if err != nil {
			return b.state(), err
		}
	}
	// update local state
	b.remaining = b.capacity - min(dbBucket.Value, b.capacity)
	b.reset = dbBucket.Expiration
	if amount > b.remaining {
		return b.state(), leakybucket.ErrorFull
	}
	updatedDBBucket, err := b.db.incrementBucketValue(b.name, amount, b.capacity)
	if err != nil {
		if err == errBucketCapacityExceeded {
			return b.state(), leakybucket.ErrorFull
		}
		return b.state(), err
	}
	// ensure we can't overflow
	b.remaining = b.capacity - min(updatedDBBucket.Value, b.capacity)
	return b.state(), nil
}

func (b *bucket) state() leakybucket.BucketState {
	return leakybucket.BucketState{
		Capacity:  b.Capacity(),
		Remaining: b.Remaining(),
		Reset:     b.Reset(),
	}
}

var _ leakybucket.Storage = &Storage{}

// Storage is a dyanamodb-based, thread-safe leaky bucket factory.
type Storage struct {
	db bucketDB
}

// Create a bucket. It will determine the current state of the bucket based on:
// - The corresponding bucket in the database
// - From scratch using the values provided
func (s *Storage) Create(name string, capacity uint, rate time.Duration) (leakybucket.Bucket, error) {
	bucket := &bucket{
		name:      name,
		capacity:  capacity,
		remaining: capacity,
		reset:     time.Now().Add(rate),
		rate:      rate,
		db:        s.db,
	}
	dbBucket, err := s.db.findOrCreateBucket(name, rate)
	if err != nil {
		return nil, err
	}
	// guarantee the bucket is in a good state
	if dbBucket.expired() {
		// adding 0 will reset the persisted bucket
		if _, err := bucket.Add(0); err != nil {
			return nil, err
		}
	}
	bucket.remaining = max(capacity-dbBucket.Value, 0)
	bucket.reset = dbBucket.Expiration

	return bucket, nil
}

// New initializes the connection to dynamodb
func New(tableName string, s *session.Session, itemTTL time.Duration) (*Storage, error) {
	ddb := dynamodb.New(s)

	db := bucketDB{
		ddb:       ddb,
		tableName: tableName,
		ttl:       itemTTL,
	}

	// fail early if the table doesn't exist or we have any other issues with the DynamoDB API
	if _, err := ddb.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}); err != nil {
		return nil, err
	}

	return &Storage{
		db: db,
	}, nil
}

func max(a, b uint) uint {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}
