package ratelimit

import (
	"testing"
	"time"

	"github.com/beeekind/ratelimit/memory"
)

var (
	defaultTestRate      = int64(1)
	defaultTestBurst     = int64(10)
	defaultTestInterval  = time.Second
	defaultMemoryBackend = memory.New()
	defaultLimiter       = New(defaultTestRate, defaultTestInterval, defaultTestBurst, defaultMemoryBackend)
)

type refillAllowanceInput struct {
	desc                            string
	currentTime                     int64
	previousAllowance               int64
	previousLastAccessedTimestampNS int64
	burst                           int64
	interval                        int64
	rate                            int64
}

type refillAllowanceOutput struct {
	expectedNewAllowance               int64
	expectedNewLastAccessedTimestampNS int64
}

var second = int64(time.Second)
var tNow = time.Now()
var now = tNow.UnixNano()
var tOneSecondAgo = tNow.Add(-1 * time.Second)
var oneSecondAgo = tOneSecondAgo.UnixNano()
var fiveSecondAgo = tNow.Add(-5 * time.Second).UnixNano()

var refillAllowanceTests = map[refillAllowanceInput]refillAllowanceOutput{
	// the following cases should not result in a refill
	{"!bucketHasRoom results in no refill", now, 6, 0, 5, 10, 10}:    {6, 0},
	{"!intervalhasPassed results in no refill", 0, 7, 0, 10, 10, 10}: {7, 0},
	// the following cases should cause a refill
	{"elapsed > 10 years results in max refill", now, 5, 0, 10, second, 1}:       {10, now},
	{"elapsed == rate results in 1 refill", now, 5, oneSecondAgo, 10, second, 1}: {6, now},
	{"should refill 5 in 5 seconds", now, 0, fiveSecondAgo, 5, second, 1}:        {5, now},
}

func TestRefillAllowance(t *testing.T) {
	for in, out := range refillAllowanceTests {
		newAllowance, newLastAccessedTimestampNS := refillAllowance(
			in.currentTime,
			in.previousAllowance,
			in.previousLastAccessedTimestampNS,
			in.burst,
			in.interval,
			in.rate,
		)

		if newAllowance != out.expectedNewAllowance {
			t.Logf("(test %s) newAllowance %v != expectedNewAllowance %v", in.desc, newAllowance, out.expectedNewAllowance)
			println(in.previousAllowance, newAllowance, out.expectedNewAllowance)

			t.Fail()
		}

		if newLastAccessedTimestampNS != out.expectedNewLastAccessedTimestampNS {
			t.Logf("(test %s) lastAccessed %v != expectedLastAccessed %v", in.desc, newLastAccessedTimestampNS, out.expectedNewLastAccessedTimestampNS)
			t.Fail()
		}
	}
}

func TestAllowsBurst(t *testing.T) {
	t.Skip()
	u1 := "Foo"

	successfulActions := 0
	for i := 0; i < 11; i++ {
		waitTime, _ := defaultLimiter.Allow(u1)
		if waitTime == 0 {
			successfulActions++
			continue
		}

		time.Sleep(waitTime)
	}

	if successfulActions != 10 {
		t.Fail()
	}
}

func TestAllowLimitEasesAfterWait(t *testing.T) {
	t.Skip()
	u2 := "Bar"

	successfulActions := 0
	failedActions := 0
	for i := 0; i < 20; i++ {
		waitTime, _ := defaultLimiter.Allow(u2)
		if waitTime == 0 {
			successfulActions++
			continue
		}

		failedActions++
		time.Sleep(waitTime)
	}

	if successfulActions != 15 {
		t.Logf("unexpected successfulActions %v != %v\n", successfulActions, 15)
		t.Fail()
	}

	if failedActions != 5 {
		t.Logf("unexpected failedActions %v != %v\n", failedActions, 5)
		t.Fail()
	}
}

func TestConcurrentUse(t *testing.T) {
	t.Skip()
	u3 := "baz"

	successCh := make(chan int)
	failureCh := make(chan int)

	simulatedUsers := 5

	for i := 0; i < simulatedUsers; i++ {
		go func(rl *RateLimit, key string, successCh chan int, failureCh chan int) {
			successes := 0
			failures := 0
			for i := 0; i < 20; i++ {
				wait, _ := rl.Allow(key)
				if wait == 0 {
					successes++
				} else {
					failures++
					time.Sleep(wait)
				}
			}

			successCh <- successes
			failureCh <- failures
		}(defaultLimiter, u3, successCh, failureCh)
	}

	totalSuccesses := 0
	totalFailures := 0
	for i := 0; i < simulatedUsers; i++ {
		totalSuccesses += <-successCh
		totalFailures += <-failureCh
	}

	if 22 > totalSuccesses || totalSuccesses > 28 {
		t.Logf("unexpected totalSuccesses the following is not true 22 < %v < 28\n", totalSuccesses)
		t.Fail()
	}

	if 72 > totalFailures || totalFailures > 77 {
		t.Logf("unexpected totalFailures the folling is not true 72 <  %v < 77\n", totalFailures)
		t.Fail()
	}
}
