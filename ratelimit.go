package ratelimit

import (
	"sync"
	"time"
)

// RateLimit ...
type RateLimit struct {
	// mu protects configuration changes in concurrent environments from interfering
	// with the Allow() method
	mu *sync.RWMutex
	// burst represents the maximum "tokens" a given Key can refill to and the maximum amount of Allowed() calls
	// that can return with a value of time.Duration(0) for any passage of (RateLimit.rate / RateLimit.burst) * RateLimit.interval
	burst int64
	// rate represents how many tokens can be refilled per RateLimit.interval up to a maximum of RateLimit.burst
	rate int64
	// interval represents the duration until the bucket should be refilled by RateLimit.rate
	interval time.Duration
	// backend is an abstraction for storing the needed data for any given Key to be ratelimited
	backend Backend
}

// Backend is an abstraction for storing the needed data for any given Key to be ratelimited
type Backend interface {
	// GetState returns an allowance representing the number of available tokens in the bucket and a lastAccessedTimestampNS representing
	// the last time a key was evaluated to be refilled 
	GetState(key string) (allowance int64, lastAccessedTimestampNS int64, err error)
	SetState(key string, allowance int64, lastAccessedTimestampNS int64) error
}

// New returns a new instance of RateLimit
func New(rate int64, interval time.Duration, burst int64, backend Backend) *RateLimit {
	return &RateLimit{
		burst:    burst,
		rate:     rate,
		interval: interval,
		backend:  backend,
		mu:       &sync.RWMutex{},
	}
}

// SetBurst adjusts RateLimit.burst using a RWMutex to lock the struct for safe concurrent use
func (rl *RateLimit) SetBurst(burst int64) {
	rl.mu.Lock()
	rl.burst = burst
	rl.mu.Unlock()
}

// SetRate adjusts RateLimit.rate using a RWMutex to lock the struct for safe concurrent use
func (rl *RateLimit) SetRate(rate int64) {
	rl.mu.Lock()
	rl.rate = rate
	rl.mu.Unlock()
}

// SetInterval adjusts RateLimit.interval using a RWMutex to lock the struct for safe concurrent use
func (rl *RateLimit) SetInterval(interval time.Duration) {
	rl.mu.Lock()
	rl.interval = interval
	rl.mu.Unlock()
}

// SetBackend adjusts RateLimit.backend using a RWMutex to lock the struct for safe concurrent use
func (rl *RateLimit) SetBackend(backend Backend) {
	rl.mu.Lock()
	rl.backend = backend
	rl.mu.Unlock()
}

// Allow returns time.Duration(timeUntilNextRefill) if the user is limited else it will return time.Duration(0).
//
// This method concurrently accesses RateLimit.rate, RateLimit.burst, and RateLimit.interval, using a
// RWMutex
//
// This method returns an error if the underlying calls to RateLimit.backend.GetState() or RateLimit.backend.SetState()
// return an error. A negative time.Duration will also be returned. The behavior of a negative time.Duration is insignificant
// when used in time.Sleep() calls, but it felt nominally important to differentiate the response of a failed Allow() beyond err != nil.
func (rl *RateLimit) Allow(key string) (nextRefill time.Duration, err error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	previousAllowance, previousLastAccessedTimestampNS, err := rl.backend.GetState(key)
	if err != nil {
		return -1, err
	}

	// get the current time as int64 represented in nanoseconds
	// WARNING: time.Now.Unix() will return a representation in seconds which requires an additional conversion to compare, so we use .UnixNano()
	currentTime := time.Now().UnixNano()

	// 1) Refill the allowance by the quantity of RateLimit.interval that has passed since lastAccessedTimestampNS
	// 2) If the refilled allowance is > RateLimit.burst, cap the refilled allowance to RateLimit.burst
	newAllowance, newLastAccessedTimestampNS := refillAllowance(
		currentTime, 
		previousAllowance,
		previousLastAccessedTimestampNS, 
		rl.burst, 
		int64(rl.interval),
		rl.rate,
	)

	// 3) If we have an allowance decrement, save the new state, and return time.Duration(0)
	if newAllowance > 0 {
		// prepare the new state
		newAllowance = newAllowance - 1

		// save the new state
		if err := rl.backend.SetState(key, newAllowance, newLastAccessedTimestampNS); err != nil {
			return -1, err
		}

		// return a duration of zero signaling that another action can begin immediately without blocking
		return time.Duration(0), nil
	}

	// 4) Else save the new state and return the time.Duration until the next refill
	if err := rl.backend.SetState(key, newAllowance, newLastAccessedTimestampNS); err != nil {
		return -1, err
	}

	// if !intervalHasPassed we can return a rl.interval - elapsed 
	intervalHasPassed := (previousLastAccessedTimestampNS + int64(rl.interval)) <= currentTime
	if !intervalHasPassed {
		elapsed := time.Duration(currentTime - previousLastAccessedTimestampNS)
		nextRefill := rl.interval - elapsed
		return nextRefill, nil  
	}

	//
	return rl.interval, nil
}

func refillAllowance(currentTime, previousAllowance, previousLastAccessedTimestampNS, burst, interval, rate int64)(newAllowance, newLastAccessedTimestampNS int64){
	bucketHasRoom := previousAllowance < burst 
	intervalHasPassed := (previousLastAccessedTimestampNS + interval) <= currentTime

	if bucketHasRoom && intervalHasPassed {
		// compute how much we should add to the allowance 
		elapsed := currentTime - previousLastAccessedTimestampNS
		intervalsPassed := elapsed / interval 
		allowanceRestored := rate * intervalsPassed

		// ensure newAllowance does not surpass burst 
		newAllowance := previousAllowance + allowanceRestored
		if newAllowance > burst {
			newAllowance = burst 
		}

		// always return currentTime
		return newAllowance, currentTime
	}

	// if no changes are made to the allowance, return the previous allowance and accessedTimestamp
	return previousAllowance, previousLastAccessedTimestampNS
}