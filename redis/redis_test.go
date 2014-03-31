package redis

import (
	"github.com/Clever/leakybucket"
	"testing"
)

func getLocalStorage() *Storage {
	storage, err := New("tcp", "localhost:6379", 0, 0)
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
	t.Skip()
	flushDb()
	leakybucket.ThreadSafeAddTest(getLocalStorage())(t)
}
