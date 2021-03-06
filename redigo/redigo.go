package redigo

// redigo implements the beeekind/ratelimit.Backend interface with gomodule/redigo/redis
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
	"strings"

	"github.com/gomodule/redigo/redis"
)

// Backend ...
type Backend struct {
	pool *redis.Pool
}

// these keys are aliased to reduce backend storage requirements
const allowanceKey = "0"
const accessedKey = "1"

// New returns a new instance of this backend
func New(pool *redis.Pool) *Backend {
	return &Backend{
		pool: pool,
	}
}

func (b *Backend) poolDo(commandName string, args ...interface{}) (reply interface{}, err error) {
	conn := b.pool.Get()
	defer conn.Close()
	reply, err = conn.Do(commandName, args...)
	return reply, err
}

// GetState retrieves allowance and lastAccessedTimestampNS from a hash set at key
func (b *Backend) GetState(key string) (allowance int64, lastAccessedTimestampNS int64, err error) {
	hashSet, err := redis.Int64Map(b.poolDo("HGETALL", key))
	if err != nil {
		// non-existent keys represent the first time Allow() is called for a given key and should return
		// zero values which will be handled properly in beeekind/ratelimit, there is some discussion that we
		// should instead return a named error that is handled more explicitly one level up the stack so this
		// behavior may change (!) in future releases
		if err.Error() == "redigo: nil returned" {
			return 0, 0, nil
		}

		return 0, 0, fmt.Errorf("failed to getState: %w", err)
	}

	allowance, allowanceExists := hashSet[allowanceKey]
	if !allowanceExists {
		return 0, 0, fmt.Errorf("failed to getState: %s hashSet did not contain key %s", key, allowanceKey)
	}

	lastAccessedTimestampNS, lastAccessedTimestampNSExists := hashSet[accessedKey]
	if !lastAccessedTimestampNSExists {
		return 0, 0, fmt.Errorf("failed to getState: %s hashSet did not contain key %s", key, accessedKey)
	}

	return allowance, lastAccessedTimestampNS, nil
}

// SetState sets allowance and lastAccessedTimestampNS as a hash set using they keys 0 and 1 to reduce
// size, respectively
func (b *Backend) SetState(key string, allowance int64, lastAccessedTimestampNS int64) error {
	if _, err := b.poolDo("HSET", key, allowanceKey, strconv.FormatInt(allowance, 10), accessedKey, strconv.FormatInt(lastAccessedTimestampNS, 10)); err != nil {
		return fmt.Errorf("failed to setState: %w", err)
	}

	return nil
}

// GetStateKey retrieves the allowance and lastAccessedTimestampNS values as a concatenated string instead
// of a hash set so we can test the performance difference between the two storage mechanisms
func (b *Backend) GetStateKey(key string) (allowance int64, lastAccessedTimestampNS int64, err error) {
	s, err := redis.String(b.poolDo("GET", key))
	if err != nil {
		// non-existent keys represent the first time Allow() is called for a given key and should return
		// zero values which will be handled properly in beeekind/ratelimit, there is some discussion that we
		// should instead return a named error that is handled more explicitly one level up the stack so this
		// behavior may change (!) in future releases
		if err.Error() == "redigo: nil returned" {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("failed to getState: %w", err)
	}

	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return 0, 0, errors.New("failed to getState: value for key not delimited by colon ':'")
	}

	allowance, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to getState: value cannot be parsed to int64: %w", err)
	}

	lastAccessedTimestampNS, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to getState: value cannot be parsed to int64: %w", err)
	}

	return allowance, lastAccessedTimestampNS, nil
}

// SetStateKey stores the allowance and lastAccessedTimestampNS values as a concatenated string instead
// of a hash set so we can test the performance difference between the two storage mechanisms
func (b *Backend) SetStateKey(key string, allowance int64, lastAccessedTimestampNS int64) error {
	if _, err := b.poolDo("SET", key, fmt.Sprintf("%v:%v", strconv.FormatInt(allowance, 10), strconv.FormatInt(lastAccessedTimestampNS, 10))); err != nil {
		return fmt.Errorf("failed to setState: %w", err)
	}

	return nil
}

// FlushAll keys for testing purposes
func (b *Backend) FlushAll() error {
	if _, err := b.poolDo("FLUSHALL"); err != nil {
		return err
	}

	return nil
}
