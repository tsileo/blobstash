package queue

import (
	"os"
	"testing"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type Item struct {
	Val string
}

func TestQueue(t *testing.T) {
	q, err := New("queue_test")
	defer func() {
		q.Close()
		os.Remove("queue_test")
	}()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}
	item1 := &Item{"ok"}
	item2 := &Item{"ok2"}
	check(q.Enqueue(item1))
	time.Sleep(1 * time.Second)
	check(q.Enqueue(item2))

	deq := &Item{}
	ok, deqFunc, err := q.Dequeue(deq)
	if !ok {
		t.Errorf("an item should have been dequeued")
	}
	check(err)
	// Don't remove the item from queue
	deqFunc(false)
	if deq.Val != "ok" {
		t.Errorf("dequeued value should be \"ok\", got \"%s\"", deq.Val)
	}

	deq1 := &Item{}
	ok1, deqFunc, err := q.Dequeue(deq1)
	if !ok1 {
		t.Errorf("an item should have been dequeued")
	}
	check(err)
	deqFunc(true)
	if deq.Val != "ok" {
		t.Errorf("dequeued value should be \"ok\", got \"%s\"", deq1.Val)
	}

	deq2 := &Item{}
	ok, deqFunc, err = q.Dequeue(deq2)
	if !ok {
		t.Errorf("an item should have been dequeued")
	}
	deqFunc(true)
	check(err)
	if deq2.Val != "ok2" {
		t.Errorf("dequeued value should be \"ok2\", got \"%s\"", deq2.Val)
	}
	deq3 := &Item{}
	ok, _, err = q.Dequeue(deq3)
	if ok {
		t.Errorf("no item should have been dequeued")
	}
	check(err)
	if deq3.Val != "" {
		t.Errorf("no item should have been dequeued, got \"%s\"", deq3.Val)
	}
}
