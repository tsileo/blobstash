package docstore

import (
	"sync"
)

// XXX(tsileo): should the locks map be bounded? or it will never cause any issue?

type locker struct {
	locks map[string]chan struct{} // Map of lock for each doc ID

	mu *sync.Mutex // Guard for the locks
}

func newLocker() *locker {
	return &locker{
		locks: map[string]chan struct{}{},
		mu:    &sync.Mutex{},
	}
}

func (l *locker) Lock(id string) {
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
			// The lock was acquired successfully, we can return
			break
		}
	}
}

func (l *locker) Unlock(id string) {
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
