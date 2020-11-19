package main

import (
	"fmt"
	"time"

	"github.com/b3ntly/ratelimit"
	"github.com/b3ntly/ratelimit/redigo"
	"github.com/gomodule/redigo/redis"
)

func main() {
	pool := &redis.Pool{
		Wait:            true,
		MaxIdle:         1,
		MaxActive:       100,
		IdleTimeout:     time.Millisecond * 500,
		MaxConnLifetime: time.Millisecond * 500,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379", redis.DialPassword("password"), redis.DialDatabase(0))
		},
	}

	backend := redigo.New(pool)
	rate := int64(1)
	interval := time.Second
	burst := int64(10)
	limiter := ratelimit.New(rate, interval, burst, backend)

	successes := 0
	failures := 0
	for i := 0; i < 15; i++ {
		wait, err := limiter.Allow("benjamin")
		if err != nil {
			println(err.Error())
			return
		}

		if wait == 0 {
			successes++
			continue
		}

		failures++
		time.Sleep(wait)
	}

	fmt.Printf("successes: %v\n", successes)
	fmt.Printf("failures: %v\n", failures)
}
