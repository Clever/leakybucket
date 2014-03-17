package leakybucket

import (
	"time"
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

// BucketFactory interface for generating buckets keyed by a string.
type BucketFactory interface {
	Create(string) Bucket
}
