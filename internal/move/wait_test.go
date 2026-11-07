package move

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitUntilStopsOnSuccessErrorTimeoutOrCancel(t *testing.T) {
	ctx := context.Background()
	calls := 0
	if err := waitUntil(ctx, time.Second, time.Millisecond, func() (bool, error) { calls++; return calls == 3, nil }); err != nil || calls != 3 {
		t.Fatalf("success on the third call: err %v after %d calls", err, calls)
	}
	boom := errors.New("boom")
	if err := waitUntil(ctx, time.Second, time.Millisecond, func() (bool, error) { return false, boom }); !errors.Is(err, boom) {
		t.Fatalf("a failing check must stop the wait with its error: %v", err)
	}
	start := time.Now()
	if err := waitUntil(ctx, 30*time.Millisecond, time.Millisecond, func() (bool, error) { return false, nil }); !errors.Is(err, errTimeout) {
		t.Fatalf("never done: %v, want a timeout", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the timeout took %v", took)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := waitUntil(cancelled, time.Hour, time.Hour, func() (bool, error) { return false, nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled context must end the wait at once: %v", err)
	}
	calls = 0
	if err := waitUntil(ctx, 0, time.Hour, func() (bool, error) { calls++; return true, nil }); err != nil || calls != 1 {
		t.Fatalf("an already finished condition succeeds even with no time left: %v", err)
	}
}
