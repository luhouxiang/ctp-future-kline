package order

import (
	"context"
	"errors"

	"ctp-go-demo/internal/logger"
)

var ErrReplayOrderBlocked = errors.New("ERR_REPLAY_ORDER_BLOCKED")

type replayKey struct{}

type ReplayMeta struct {
	EventID      string
	ReplayTaskID string
}

func WithReplayMeta(ctx context.Context, meta ReplayMeta) context.Context {
	return context.WithValue(ctx, replayKey{}, meta)
}

func ReplayMetaFromContext(ctx context.Context) (ReplayMeta, bool) {
	v := ctx.Value(replayKey{})
	meta, ok := v.(ReplayMeta)
	return meta, ok
}

func IsReplayContext(ctx context.Context) bool {
	_, ok := ReplayMetaFromContext(ctx)
	return ok
}

func EnsureReplaySafe(ctx context.Context, commandID string) error {
	meta, ok := ReplayMetaFromContext(ctx)
	if !ok {
		return nil
	}
	logger.Error(
		"replay order blocked",
		"command_id", commandID,
		"event_id", meta.EventID,
		"replay_task_id", meta.ReplayTaskID,
	)
	return ErrReplayOrderBlocked
}
