package memory

import "sync"

// Backend ...
type Backend struct {
	mu   *sync.RWMutex
	data map[string]*state
}

type state struct {
	allowance              int64
	lastAllowedTimestampNS int64
}

// New returns a new instance of memory.Backend 
func New() *Backend {
	return &Backend{
		mu:   &sync.RWMutex{},
		data: make(map[string]*state),
	}
}

// GetState ...
func (b *Backend) GetState(key string) (allowance int64, lastAllowedTimestampNS int64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, exists := b.data[key]
	if !exists {
		return 0, 0, nil 
	}
	return data.allowance, data.lastAllowedTimestampNS, nil
}

// SetState ...
func (b *Backend) SetState(key string, allowance int64, lastAllowedTimestampNS int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data[key] = &state{
		allowance:              allowance,
		lastAllowedTimestampNS: lastAllowedTimestampNS,
	}
	return nil
}
