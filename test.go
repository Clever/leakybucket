package leakybucket

import (
	"math"
	"sync"
	"testing"
	"time"
)

// CreateTest returns a test of bucket creation for a given storage backend.
// It is meant to be used by leakybucket implementers who wish to test this.
func CreateTest(s Storage, name string) func(*testing.T) {
	return func(t *testing.T) {
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

// ThreadSafeAddTest returns a test that adding to a single bucket is thread-safe.
// It is meant to be used by leakybucket implementers who wish to test this.
func ThreadSafeAddTest(s Storage, name string) func(*testing.T) {
	return func(t *testing.T) {
		// Make a bucket of size `n`. Spawn `n+1` goroutines that each try to take one token.
		// We should see the bucket transition through having `n-1`, `n-2`, ... 0 remaining capacity.
		// We should also witness one error when the bucket has reached capacity.
		n := 100
		bucket, err := s.Create("testbucket", uint(n), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		remaining := map[uint]bool{}     // record observed "remaining" counts. (ab)using map as set here
		remainingMutex := sync.RWMutex{} // maps are not threadsafe
		errors := []error{}              // record observed errors
		var wg sync.WaitGroup
		for i := 0; i < n+1; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				state, err := bucket.Add(1)
				if err != nil {
					errors = append(errors, err)
				} else {
					remainingMutex.Lock()
					defer remainingMutex.Unlock()
					remaining[state.Remaining] = true
				}
			}()
		}
		wg.Wait()
		if len(remaining) != n {
			t.Fatalf("Did not observe correct bucket states: %#v, %#v", remaining, errors)
		}
		if len(errors) != 1 && errors[0] != ErrorFull {
			t.Fatalf("Did not observe one full error: %#v", errors)
		}
	}
}
