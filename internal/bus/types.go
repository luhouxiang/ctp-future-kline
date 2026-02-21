package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

const (
	TopicTick         = "tick"
	TopicBar          = "bar"
	TopicOrderCommand = "order_command"
	TopicOrderStatus  = "order_status"
)

type BusEvent struct {
	EventID      string          `json:"event_id"`
	Topic        string          `json:"topic"`
	Source       string          `json:"source"`
	OccurredAt   time.Time       `json:"occurred_at"`
	ProducedAt   time.Time       `json:"produced_at"`
	Replay       bool            `json:"replay"`
	ReplayTaskID string          `json:"replay_task_id,omitempty"`
	Payload      json.RawMessage `json:"payload"`
}

type FileCursor struct {
	File   string `json:"file"`
	Offset int64  `json:"offset"`
}

type ReadOptions struct {
	StartTime  *time.Time
	EndTime    *time.Time
	Topics     map[string]struct{}
	Sources    map[string]struct{}
	FromCursor *FileCursor
}

type EventHandler func(ctx context.Context, ev BusEvent, cursor FileCursor) error

func BuildSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(values))
	for _, item := range values {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		out[item] = struct{}{}
	}
	return out
}

func ShouldKeepEvent(ev BusEvent, opts ReadOptions) bool {
	if opts.StartTime != nil {
		ts := eventTime(ev)
		if ts.Before(*opts.StartTime) {
			return false
		}
	}
	if opts.EndTime != nil {
		ts := eventTime(ev)
		if ts.After(*opts.EndTime) {
			return false
		}
	}
	if len(opts.Topics) > 0 {
		if _, ok := opts.Topics[ev.Topic]; !ok {
			return false
		}
	}
	if len(opts.Sources) > 0 {
		if _, ok := opts.Sources[ev.Source]; !ok {
			return false
		}
	}
	return true
}

func eventTime(ev BusEvent) time.Time {
	if !ev.OccurredAt.IsZero() {
		return ev.OccurredAt
	}
	return ev.ProducedAt
}

var eventSeq atomic.Uint64

func NewEventID() string {
	now := time.Now().UTC().Format("20060102T150405.000000000")
	seq := eventSeq.Add(1)
	return fmt.Sprintf("%s-%016x", now, seq)
}
