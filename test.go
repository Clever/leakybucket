package leakybucket

import (
	"math"
	"sync"
	"testing"
	"time"
)

// CreateTest returns a test of bucket creation for a given storage backend.
// It is meant to be used by leakybucket implementers who wish to test this.
func CreateTest(s Storage) func(*testing.T) {
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

// AddTest returns a test that adding to a single bucket works.
// It is meant to be used by leakybucket implementers who wish to test this.
func AddTest(s Storage) func(*testing.T) {
	return func(t *testing.T) {
		bucket, err := s.Create("testbucket", 10, time.Minute)
		if err != nil {
			t.Fatal(err)
		}

		addAndTestRemaining := func(add, remaining uint) {
			if state, err := bucket.Add(add); err != nil {
				t.Fatal(err)
			} else if bucket.Remaining() != state.Remaining {
				t.Fatalf("expected bucket and state remaining to match, bucket is %d, state is %d",
					bucket.Remaining(), state.Remaining)
			} else if state.Remaining != remaining {
				t.Fatalf("expected %d remaining, got %d", remaining, state.Remaining)
			}
		}

		addAndTestRemaining(1, 9)
		addAndTestRemaining(3, 6)
		addAndTestRemaining(6, 0)

		if _, err := bucket.Add(1); err == nil {
			t.Fatalf("expected ErrorFull, received no error")
		} else if err != ErrorFull {
			t.Fatalf("expected ErrorFull, received %v", err)
		}
	}
}

func AddResetTest(s Storage) func(*testing.T) {
	return func(t *testing.T) {
		bucket, err := s.Create("testbucket", 1, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bucket.Add(1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond * 2)
		if state, err := bucket.Add(1); err != nil {
			t.Fatal(err)
		} else if state.Remaining != 0 {
			t.Fatalf("expected full bucket, got %d", state.Remaining)
		} else if state.Reset.Unix() < time.Now().Unix() {
			t.Fatalf("reset time is in the past")
		}
	}
}

// ThreadSafeAddTest returns a test that adding to a single bucket is thread-safe.
// It is meant to be used by leakybucket implementers who wish to test this.
func ThreadSafeAddTest(s Storage) func(*testing.T) {
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
			keys := []uint{}
			for key := range remaining {
				keys = append(keys, key)
			}
			t.Fatalf("Did not observe correct bucket states. Saw %d distinct remaining values instead of %d: %v",
				len(remaining), n, keys)
		}
		if !(len(errors) == 1 && errors[0] == ErrorFull) {
			t.Fatalf("Did not observe one full error: %#v", errors)
		}
	}
}
