package leakybucket

import (
	"math"
	"testing"
	"time"
)

// CreateTest returns a test of bucket creation for a given storage backend.
// It is meant to be used by leakybucket implementers.
func CreateTest(s Storage, name string) func(*testing.T) {
	return func(t *testing.T) {
		t.Logf("Testing %s Create", name)
		now := time.Now()
		bucket, err := s.Create("testbucket", 100, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if capacity := bucket.Capacity(); capacity != 100 {
			t.Fatalf("expected capacity of %d, got %d", 100, capacity)
		}
		e := float64(1 * time.Second) // margin of error
		if error := float64(bucket.Reset().Sub(now.Add(time.Minute))); math.Abs(error) > e {
			t.Fatalf("expected reset time close to %s, got %s", now.Add(time.Minute),
				bucket.Reset())
		}
	}
}
