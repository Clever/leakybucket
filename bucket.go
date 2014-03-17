package leakybucket

import (
	"errors"
	"time"
)

var (
	// ErrorFull is returned when the amount requested to add exceeds the remaining space in the bucket.
	ErrorFull = errors.New("add exceeds free capacity")
)

// Bucket interface for interacting with leaky buckets: https://en.wikipedia.org/wiki/Leaky_bucket
type Bucket interface {
	// Capcity of the bucket.
	Capacity() uint

	// Remaining space in the bucket.
	Remaining() uint

	// Reset returns when the bucket will be drained.
	Reset() time.Time

	// Add to the bucket.
	Add(uint) error
}

// Storage interface for generating buckets keyed by a string.
type Storage interface {
	// Create a bucket with a name, capacity, and rate.
	// rate is how long it takes for full capacity to drain.
	Create(name string, capacity uint, rate time.Duration) (Bucket, error)
}
