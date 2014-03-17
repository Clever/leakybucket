package leakybucket

import (
	"testing"
	"time"
)

// CreateTest returns a tests the bucket returned by a bucketfactory.
func CreateTest(bf BucketFactory, name string) func(*testing.T) {
	return func(t *testing.T) {
		t.Logf("Testing %s Create", name)
		bucket := bf.Create("testbucket", 100, time.Minute)
		if err := bucket.Add(10); err != nil {
			t.Fatalf("add failed: %s", err)
		}
		if err := bucket.Add(100); err != ErrorFull {
			t.Fatal("expected error when adding beyond capacity")
		}
		if capacity := bucket.Capacity(); capacity != 100 {
			t.Fatalf("expected capacity of %d, got %d", 100, capacity)
		}
		// TODO: test that reset time is roughly a miniute from now
	}
}
