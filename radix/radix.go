package radix

// redigo implements the beeekind/ratelimit.Backend interface with mediocregopher/radix/v3
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
	"errors"
	"fmt"
	"strconv"

	"github.com/mediocregopher/radix/v3"
)

// Backend ...
type Backend struct {
	pool *radix.Pool
}

// these keys are aliased to reduce storage requirements
const allowanceKey = "0"
const accessedKey = "1"

// New returns a new instance of radix.Backend
func New(pool *radix.Pool) *Backend {
	return &Backend{
		pool: pool,
	}
}

// SetState ...
func (b *Backend) SetState(key string, allowance int64, lastAccessedTimestampNS int64) error {
	var result string
	if err := b.pool.Do(radix.Cmd(&result, "HSET", key, allowanceKey, strconv.FormatInt(allowance, 10), accessedKey, strconv.FormatInt(lastAccessedTimestampNS, 10))); err != nil {
		return err
	}

	return nil
}

// GetState ...
func (b *Backend) GetState(key string) (allowance int64, lastAllowedTimeStampNS int64, err error) {
	var hashSet map[string]string
	if err := b.pool.Do(radix.Cmd(&hashSet, "HGETALL", key)); err != nil {
		return 0, 0, fmt.Errorf("failed to getState: %w", err)
	}

	allowanceStr, allowanceExists := hashSet[allowanceKey]
	lastAllowedTimeStampNSStr, lastAllowedTimeStampNSExists := hashSet[accessedKey]
	if allowanceExists || lastAllowedTimeStampNSExists {
		return 0, 0, errors.New("failed to getState: hashSet did not contain key")
	}

	allowance, err = strconv.ParseInt(allowanceStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to getState: value could not be parsed into int64: %w", err)
	}

	lastAllowedTimeStampNS, err = strconv.ParseInt(lastAllowedTimeStampNSStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to getState: value could not be parsed into int64: %w", err)
	}

	return allowance, lastAllowedTimeStampNS, err
}

// FlushAll ...
func (b *Backend) FlushAll() (string, error) {
	var keysFlushed string
	err := b.pool.Do(radix.Cmd(&keysFlushed, "FLUSHALL"))
	return keysFlushed, err
}
