package hub

import "sync"

type Hub struct {
	subs map[string]map[chan string]bool
	sync.Mutex
}

func NewHub() *Hub {
	return &Hub{
		subs: map[string]map[chan string]bool{},
	}
}

func (h *Hub) Pub(key, message string) int {
	h.Lock()
	defer h.Unlock()
	if _, ok := h.subs[key]; !ok {
		return 0
	}
	sent := 0
	for c, _ := range h.subs[key] {
		c <- message
		sent++
	}
	return sent
}

func (h *Hub) Sub(key string) chan string {
	h.Lock()
	defer h.Unlock()
	c := make(chan string)
	if _, ok := h.subs[key]; !ok {
		h.subs[key] = map[chan string]bool{}
	}
	h.subs[key][c] = true
	return c
}

func (h *Hub) Unsub(key string, c chan string) bool {
	h.Lock()
	defer h.Unlock()
	if _, ok := h.subs[key]; !ok {
		return false
	}
	if _, ok := h.subs[key][c]; ok {
		delete(h.subs[key], c)
		return true
	}
	return false
}
