package docstore

import (
	"sync"
)

// XXX(tsileo): should this be exported?
type Locker struct {
	locks map[string]chan struct{} // Map of lock for each doc ID

	mu *sync.Mutex // Guard for the locks
}

func NewLocker() *Locker {
	return &Locker{
		locks: map[string]chan struct{}{},
		mu:    &sync.Mutex{},
	}
}

func (l *Locker) Lock(id string) {
	for {
		l.mu.Lock()
		// Try to retrieve the existing lock
		lchan, ok := l.locks[id]
		if !ok {
			// It does not exists, create it
			l.locks[id] = make(chan struct{})
		}
		l.mu.Unlock()

		// Now check the lock state
		if ok {
			// Try to read from the chan, it will block until the channel is closed
			// (i.e. the lock has been released)
			<-lchan
		} else {
			// The lock was acquired sucessfully, we can return
			break
		}
	}
}

func (l *Locker) Unlock(id string) {
	l.mu.Lock()
	// Try to retrieve the existing lock
	lchan, ok := l.locks[id]
	if !ok {
		panic("trying to unlock an unlocked lock")
	}
	delete(l.locks, id)
	l.mu.Unlock()

	// Close the channel, so the read that are blocking returns an empty struct and let the other
	// goroutine acquire the lock
	close(lchan)
}
