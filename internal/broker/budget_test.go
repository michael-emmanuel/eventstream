package broker

import (
	"context"
	"testing"
	"time"
)

func TestByteBudgetBoundsMemory(t *testing.T) {
	b := newByteBudget(100)
	ctx := context.Background()
	if !b.acquire(ctx, 60) || !b.acquire(ctx, 40) {
		t.Fatal("within budget")
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if b.acquire(short, 1) {
		t.Fatal("exceeded budget")
	}
	done := make(chan bool)
	go func() { done <- b.acquire(ctx, 50) }()
	time.Sleep(10 * time.Millisecond)
	b.release(60)
	if !<-done {
		t.Fatal("waiter not admitted after release")
	}
	if b.inUse() != 90 {
		t.Fatalf("in use %d", b.inUse())
	}
}

func TestOversizedRequestAdmittedAlone(t *testing.T) {
	b := newByteBudget(100)
	if !b.acquire(context.Background(), 500) {
		t.Fatal("a single oversized request must not be starved forever")
	}
}
