package radix

// redigo implements the b3ntly/ratelimit.Backend interface with mediocregopher/radix/v3
//
// redigo implements the ratelimit.Backend interface by storing the values of
// allowance and lastAccessedTimestampNS using the redis hash set data structure or
// as a concatenated string
//
// I recommend paying close attention to your redis.Pool configuration as it can severely
// impact performance. Connection handling and pooling is by far the biggest bottleneck and
// not one this library will solve for you.
//
// Note that we are coercing a int64 value to and from a string using strconv.ParseInt(val, 10, 64)
// and strconv.FormatInt(val, 10).

import (
	"fmt"
	"strconv"

	"github.com/mediocregopher/radix/v3"
)

// Backend ...
type Backend struct {
	pool *radix.Pool
}

// New returns a new instance of radix.Backend 
func New(pool *radix.Pool) *Backend {
	return &Backend{
		pool: pool,
	}
}

// SetState ...
func (b *Backend) SetState(key string, allowance int64, lastAllowedTimestampNS int64) error {
	var result string
	if err := b.pool.Do(radix.Cmd(&result, "HSET", key, "0", fmt.Sprint(allowance), "1", fmt.Sprint(lastAllowedTimestampNS))); err != nil {
		return err
	}

	return nil
}

// GetState ...
func (b *Backend) GetState(key string) (allowance int64, lastAllowedTimeStampNS int64, err error) {
	var hset map[string]string
	if err := b.pool.Do(radix.Cmd(&hset, "HGETALL", key)); err != nil {
		return 0, 0, err
	}

	a, ok := hset["0"]
	c, ok2 := hset["1"]
	if !ok || !ok2 {
		return 0, 0, nil
	}

	a1, err := strconv.Atoi(a)
	l1, err := strconv.Atoi(c)

	return int64(a1), int64(l1), err
}

// FlushAll ...
func (b *Backend) FlushAll() (string, error) {
	var keysFlushed string
	err := b.pool.Do(radix.Cmd(&keysFlushed, "FLUSHALL"))
	return keysFlushed, err
}
