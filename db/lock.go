// A slotted lock / mutex inspired by kyotocabinet SlottedRWLock
// https://groups.google.com/d/msg/golang-nuts/jjjvXG4HdUw/zr0YXKQErAYJ

package db

import (
	"github.com/spaolacci/murmur3"
	"sync"
)

// 1024 slots
const size = 0x3ff

type SlottedMutex struct {
	mutexes []sync.Mutex
}

func NewSlottedMutex() *SlottedMutex {
	return &SlottedMutex{make([]sync.Mutex, 1024)}
}

func (m *SlottedMutex) Lock(key []byte) {
	m.getLock(key).Lock()
}

func (m *SlottedMutex) Unlock(key []byte) {
	m.getLock(key).Unlock()
}

func (m *SlottedMutex) getLock(key []byte) *sync.Mutex {
	return &m.mutexes[int(murmur3.Sum32(key))%size]
}
