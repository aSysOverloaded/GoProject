package startup

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errDown = errors.New("connection refused")

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	calls := 0
	err := Retry("dep", 5*time.Second, func(context.Context) error {
		calls++
		if calls < 3 {
			return errDown
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Retry = %v, want success on the third attempt", err)
	}
	if calls != 3 {
		t.Errorf("attempts = %d, want 3", calls)
	}
}

func TestRetryGivesUpAtItsDeadline(t *testing.T) {
	start := time.Now()
	err := Retry("dep", 600*time.Millisecond, func(context.Context) error { return errDown })

	if err == nil {
		t.Fatal("Retry succeeded against a dependency that is always down")
	}
	if !errors.Is(err, errDown) {
		t.Errorf("error %v does not wrap the last failure", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %v to give up with a 600ms budget", elapsed)
	}
}

// TestRetryBoundsEachAttempt guards against one hung call - a connection that
// never answers - consuming the whole budget before a second attempt is made.
func TestRetryBoundsEachAttempt(t *testing.T) {
	calls := 0
	start := time.Now()
	err := Retry("dep", 1500*time.Millisecond, func(ctx context.Context) error {
		calls++
		<-ctx.Done() // hang until the per-attempt timeout ends it
		return ctx.Err()
	})

	if err == nil {
		t.Fatal("Retry succeeded although every attempt hung")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("took %v with a 1.5s budget: a hung attempt was not bounded", elapsed)
	}
}

func TestRetryAlwaysMakesOneAttempt(t *testing.T) {
	calls := 0
	if err := Retry("dep", 0, func(context.Context) error { calls++; return nil }); err != nil {
		t.Fatalf("Retry = %v", err)
	}
	if calls != 1 {
		t.Errorf("attempts = %d, want 1 even with a zero budget", calls)
	}
}
