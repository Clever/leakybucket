package redis

import (
	"github.com/Clever/leakybucket"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"time"
)

type bucket struct {
	name                string
	capacity, remaining uint
	reset               time.Time
	rate                time.Duration
	pool                *redis.Pool
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

func (b *bucket) State() leakybucket.BucketState {
	return leakybucket.BucketState{b.Capacity(), b.Remaining(), b.Reset()}
}

func byteArrayToUint(arr []uint8) (uint, error) {
	if num, err := strconv.Atoi(string(arr)); err != nil {
		return 0, err
	} else {
		return uint(num), err
	}
}

var millisecond = int64(time.Millisecond)

func (b *bucket) updateOldReset() error {
	conn := b.pool.Get()
	defer conn.Close()

	if b.reset.Unix() > time.Now().Unix() {
		return nil
	}
	ttl, err := conn.Do("PTTL", b.name)
	if err != nil {
		return err
	}
	b.reset = time.Now().Add(time.Duration(ttl.(int64) * millisecond))
	return nil
}

// Add to the bucket.
func (b *bucket) Add(amount uint) (leakybucket.BucketState, error) {
	conn := b.pool.Get()
	defer conn.Close()

	if count, err := conn.Do("GET", b.name); err != nil {
		return b.State(), err
	} else if count == nil {
		b.remaining = b.capacity
	} else if num, err := byteArrayToUint(count.([]uint8)); err != nil {
		return b.State(), err
	} else {
		b.remaining = b.capacity - min(uint(num), b.capacity)
	}

	if amount > b.remaining {
		b.updateOldReset()
		return b.State(), leakybucket.ErrorFull
	}

	// If SETNX doesn't return nil, we just set the key. Otherwise, it already exists.
	// Go y u no have Milliseconds method? Why only Seconds and Nanoseconds?
	if set, err := conn.Do("SET", b.name, amount, "NX", "PX", int(b.rate.Nanoseconds()/millisecond)); err != nil {
		return b.State(), err
	} else if set != nil {
		b.remaining = b.capacity - amount
		b.reset = time.Now().Add(b.rate)
		return b.State(), nil
	}

	b.updateOldReset()

	count, err := conn.Do("INCRBY", b.name, amount)
	if err != nil {
		return b.State(), err
	}

	// Ensure we can't overflow
	b.remaining = b.capacity - min(uint(count.(int64)), b.capacity)
	return b.State(), nil
}

// Storage is a redis-based, non thread-safe leaky bucket factory.
type Storage struct {
	pool *redis.Pool
}

// Create a bucket.
func (s *Storage) Create(name string, capacity uint, rate time.Duration) (leakybucket.Bucket, error) {
	conn := s.pool.Get()
	defer conn.Close()

	if count, err := conn.Do("GET", name); err != nil {
		return nil, err
	} else if count == nil {
		b := &bucket{
			name:      name,
			capacity:  capacity,
			remaining: capacity,
			reset:     time.Now().Add(rate),
			rate:      rate,
			pool:      s.pool,
		}
		return b, nil
	} else if num, err := byteArrayToUint(count.([]uint8)); err != nil {
		return nil, err
	} else if ttl, err := conn.Do("PTTL", name); err != nil {
		return nil, err
	} else {
		b := &bucket{
			name:      name,
			capacity:  capacity,
			remaining: capacity - min(capacity, num),
			reset:     time.Now().Add(time.Duration(ttl.(int64) * millisecond)),
			rate:      rate,
			pool:      s.pool,
		}
		return b, nil
	}
}

// New initializes the connection to redis.
func New(network, address string) (*Storage, error) {
	return &Storage{
		pool: redis.NewPool(func() (redis.Conn, error) {
			return redis.Dial(network, address)
		}, 5),
	}, nil
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}
