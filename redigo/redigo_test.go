package redigo

import (
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

var (
	// use two pools so that we don't cross keys
	poolOne = &redis.Pool{
		Wait:            true,
		MaxIdle:         1,
		MaxActive:       100,
		IdleTimeout:     time.Millisecond * 500,
		MaxConnLifetime: time.Millisecond * 500,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379", redis.DialPassword("password"), redis.DialDatabase(0))
		},
	}
	poolTwo = &redis.Pool{
		Wait:            true,
		MaxIdle:         1,
		MaxActive:       100,
		IdleTimeout:     time.Millisecond * 500,
		MaxConnLifetime: time.Millisecond * 500,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379", redis.DialPassword("password"), redis.DialDatabase(1))
		},
	}
	backendOne = New(poolOne)
	backendTwo = New(poolTwo)
)

func TestSetState(t *testing.T){
	key := "foo"
	allowance := int64(5) 
	ts := time.Now().UnixNano()
	if err := backendOne.SetState(key, allowance, ts); err != nil {
		t.Log(err.Error())
		t.Fail() 
	}

	foundAllowance, foundTs, err := backendOne.GetState(key)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
	}

	if allowance != foundAllowance {
		t.Logf("allowance %v != foundAllowance %v", allowance, foundAllowance)
		t.Fail()
	}

	if ts != foundTs {
		t.Logf("ts %v != foundTs %v", ts, foundTs)
		t.Fail()
	}
}

func TestSetStateKey(t *testing.T){
	key := "foo"
	allowance := int64(5) 
	ts := time.Now().UnixNano()
	if err := backendTwo.SetStateKey(key, allowance, ts); err != nil {
		t.Log(err.Error())
		t.Fail() 
	}

	foundAllowance, foundTs, err := backendTwo.GetStateKey(key)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
	}

	if allowance != foundAllowance {
		t.Logf("allowance %v != foundAllowance %v", allowance, foundAllowance)
		t.Fail()
	}

	if ts != foundTs {
		t.Logf("ts %v != foundTs %v", ts, foundTs)
		t.Fail()
	}
}

func BenchmarkSetState(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := backendOne.SetState(strconv.Itoa(i), 10, 10)
		if err != nil {
			b.Log(err.Error())
			b.FailNow()
		}
	}
}

func BenchmarkGetState(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, err := backendOne.GetState(strconv.Itoa(i))
		if err != nil {
			b.Log(err.Error())
			b.FailNow()
		}
	}
}

func BenchmarkSetStateKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := backendTwo.SetStateKey(strconv.Itoa(i), 10, 10)
		if err != nil {
			b.Log(err.Error())
			b.FailNow()
		}
	}
}

func BenchmarkGetStateKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, err := backendTwo.GetStateKey(strconv.Itoa(i))
		if err != nil {
			b.Log(err.Error())
			b.FailNow()
		}
	}
}
