package redis

import (
	"github.com/Clever/leakybucket"
	"testing"
)

func getLocalStorage() *Storage {
	storage, err := New("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	return storage
}

func flushDb() {
	storage := getLocalStorage()
	conn := storage.pool.Get()
	defer conn.Close()
	_, err := conn.Do("FLUSHDB")
	if err != nil {
		panic(err)
	}
}

func TestCreate(t *testing.T) {
	flushDb()
	leakybucket.CreateTest(getLocalStorage())(t)
}

func TestAdd(t *testing.T) {
	flushDb()
	leakybucket.AddTest(getLocalStorage())(t)
}

func TestThreadSafeAdd(t *testing.T) {
	// Redis Add is not thread safe. If you run this, the test should fail because it never received
	// ErrorFull. It's not thread safe because we don't atomically check the state of the bucket and
	// increment.
	t.Skip()
	flushDb()
	leakybucket.ThreadSafeAddTest(getLocalStorage())(t)
}

func TestReset(t *testing.T) {
	flushDb()
	leakybucket.AddResetTest(getLocalStorage())(t)
}

func TestFindOrCreate(t *testing.T) {
	flushDb()
	leakybucket.FindOrCreateTest(getLocalStorage())(t)
}

func TestBucketInstanceConsistencyTest(t *testing.T) {
	flushDb()
	leakybucket.BucketInstanceConsistencyTest(getLocalStorage())(t)
}
