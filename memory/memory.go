package memory

import (
	"github.com/Clever/leakybucket"
	"time"
)

type bucket struct {
	capacity  uint
	remaining uint
	reset     time.Time
	rate      time.Duration
}

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
func (b *bucket) Add(amount uint) error {
	if time.Now().After(b.reset) {
		b.reset = time.Now().Add(b.rate)
		b.remaining = b.capacity
	}
	if amount > b.remaining {
		return leakybucket.ErrorFull
	}
	b.remaining -= amount
	return nil
}

// BucketFactory is a non thread-safe in-memory leaky bucket factory.
type BucketFactory struct {
	buckets map[string]*bucket
}

// New initializes the in-memory bucket store.
func New() *BucketFactory {
	return &BucketFactory{
		buckets: make(map[string]*bucket),
	}
}

// Create a bucket.
func (bf *BucketFactory) Create(name string, capacity uint, rate time.Duration) leakybucket.Bucket {
	b, ok := bf.buckets[name]
	if ok {
		return b
	}
	b = &bucket{
		capacity:  capacity,
		remaining: capacity,
		reset:     time.Now().Add(rate),
		rate:      rate,
	}
	bf.buckets[name] = b
	return b
}
