package redigo

// redigo implements the b3ntly/ratelimit.Backend interface with gomodule/redigo/redis
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

// New returns a new instance of this backend
func New(pool *redis.Pool) *Backend {
	return &Backend{
		pool: pool,
	}
}

func (b *Backend) poolDo(commandName string, args ...interface{}) (reply interface{}, err error) {
	c := b.pool.Get()
	reply, err = c.Do(commandName, args...)
	c.Close()
	return reply, err
}

// GetState retrieves allowance and lastAccessedTimestampNS from a hash set at key
func (b *Backend) GetState(key string) (allowance int64, lastAccessedTimestampNS int64, err error) {
	m, err := redis.Int64Map(b.poolDo("HGETALL", key))
	if err != nil {
		return 0, 0, err
	}

	allowance, allowanceExists := m["0"]
	lastAccessedTimestampNS, lastAccessedTimestampNSExists := m["1"]
	if !allowanceExists || !lastAccessedTimestampNSExists {
		return 0, 0, nil
	}

	return allowance, lastAccessedTimestampNS, nil
}

// SetState sets allowance and lastAccessedTimestampNS as a hash set using they keys 0 and 1 to reduce
// size, respectively
func (b *Backend) SetState(key string, allowance int64, lastAccessedTimestampNS int64) error {
	if _, err := b.poolDo("HSET", key, "0", strconv.FormatInt(allowance, 10), "1", strconv.FormatInt(lastAccessedTimestampNS, 10)); err != nil {
		return err
	}

	return nil
}

// GetStateKey retrieves the allowance and lastAccessedTimestampNS values as a concatenated string instead
// of a hash set so we can test the performance difference between the two storage mechanisms
func (b *Backend) GetStateKey(key string) (allowance int64, lastAccessedTimestampNS int64, err error) {
	s, err := redis.String(b.poolDo("GET", key))
	if err != nil {
		if err.Error() == "redigo: nil returned" {
			return 0, 0, nil
		}

		return 0, 0, err
	}

	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid value")
	}

	allowance, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, errors.New("non integer as allowance")
	}

	lastAccessedTimestampNS, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, errors.New("non integer as lastAccessedTimestampNS")
	}

	return allowance, lastAccessedTimestampNS, nil
}

// SetStateKey stores the allowance and lastAccessedTimestampNS values as a concatenated string instead
// of a hash set so we can test the performance difference between the two storage mechanisms
func (b *Backend) SetStateKey(key string, allowance int64, lastAccessedTimestampNS int64) error {
	if _, err := b.poolDo("SET", key, fmt.Sprintf("%v:%v", strconv.FormatInt(allowance, 10), strconv.FormatInt(lastAccessedTimestampNS, 10))); err != nil {
		return err
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
