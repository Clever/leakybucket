package redis

import (
	"github.com/Clever/leakybucket"
	"github.com/garyburd/redigo/redis"
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

// Add to the bucket.
func (b *bucket) Add(amount uint) (leakybucket.BucketState, error) {
	conn := b.pool.Get()
	defer conn.Close()

	if tokens, err := conn.Do("GET", b.name); err != nil {
		return b.State(), err
	} else if tokens != nil && uint(tokens.([]uint8)[0])+amount > b.capacity {
		return b.State(), leakybucket.ErrorFull
	}

	if set, err := conn.Do("SET", b.name, amount, "NX", "EX", int(b.rate.Seconds())); err != nil {
		return b.State(), err
	} else if set != nil {
		b.remaining -= amount
		return b.State(), nil
	}

	if count, err := conn.Do("INCRBY", b.name, amount); err != nil {
		return b.State(), err
	} else {
		b.remaining = b.capacity - uint(count.(int64))
		return b.State(), nil
	}
}

type Storage struct {
	pool *redis.Pool
}

// Create a bucket.
func (s *Storage) Create(name string, capacity uint, rate time.Duration) (leakybucket.Bucket, error) {
	b := &bucket{
		name:      name,
		capacity:  capacity,
		remaining: capacity,
		reset:     time.Now().Add(rate),
		rate:      rate,
		pool:      s.pool,
	}
	return b, nil
}

// New initializes the connection to redis.
func New(network, address string, readTimeout, writeTimeout time.Duration) (*Storage, error) {
	return &Storage{
		pool: redis.NewPool(func() (redis.Conn, error) {
			return redis.Dial(network, address)
		}, 5),
	}, nil
}
