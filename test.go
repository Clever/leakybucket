package leakybucket

import (
	"testing"
	"time"
)

// CreateTest returns a test of bucket creation for a given storage backend.
// It is meant to be used by leakybucket implementers.
func CreateTest(s Storage, name string) func(*testing.T) {
	return func(t *testing.T) {
		t.Logf("Testing %s Create", name)
		bucket, err := s.Create("testbucket", 100, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if capacity := bucket.Capacity(); capacity != 100 {
			t.Fatalf("expected capacity of %d, got %d", 100, capacity)
		}
		// TODO: test that reset time is roughly a miniute from now
	}
}
