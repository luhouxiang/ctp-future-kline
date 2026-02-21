package order_test

import (
	"context"
	"errors"
	"testing"

	"ctp-go-demo/internal/order"
)

func TestEnsureReplaySafe(t *testing.T) {
	t.Parallel()

	if err := order.EnsureReplaySafe(context.Background(), "cmd-1"); err != nil {
		t.Fatalf("unexpected error for non-replay context: %v", err)
	}

	ctx := order.WithReplayMeta(context.Background(), order.ReplayMeta{
		EventID:      "ev-1",
		ReplayTaskID: "task-1",
	})
	err := order.EnsureReplaySafe(ctx, "cmd-1")
	if !errors.Is(err, order.ErrReplayOrderBlocked) {
		t.Fatalf("error = %v, want ErrReplayOrderBlocked", err)
	}
}
