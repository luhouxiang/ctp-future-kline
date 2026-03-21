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
	// EventID 是总线事件唯一标识。
	EventID string `json:"event_id"`
	// Topic 表示事件主题，例如 tick、bar、order_command。
	Topic string `json:"topic"`
	// Source 表示事件来源模块。
	Source string `json:"source"`
	// OccurredAt 是业务上事件发生的时间。
	OccurredAt time.Time `json:"occurred_at"`
	// ProducedAt 是事件被写入总线日志的时间。
	ProducedAt time.Time `json:"produced_at"`
	// Replay 标记该事件是否来自回放链路。
	Replay bool `json:"replay"`
	// ReplayTaskID 标记该事件所属的回放任务。
	ReplayTaskID string `json:"replay_task_id,omitempty"`
	// Payload 保存事件具体业务载荷。
	Payload json.RawMessage `json:"payload"`
}

type FileCursor struct {
	// File 是事件所在的日志文件路径。
	File string `json:"file"`
	// Offset 是事件在文件中的字节偏移。
	Offset int64 `json:"offset"`
}

type ReadOptions struct {
	// StartTime 是过滤事件的起始时间。
	StartTime *time.Time
	// EndTime 是过滤事件的结束时间。
	EndTime *time.Time
	// Topics 是允许保留的 topic 集合。
	Topics map[string]struct{}
	// Sources 是允许保留的 source 集合。
	Sources map[string]struct{}
	// FromCursor 是起始读取游标。
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
