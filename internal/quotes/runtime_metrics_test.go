package quotes

import (
	"testing"
	"time"
)

func TestRuntimeMetricTimeUsesNowForReplay(t *testing.T) {
	now := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)
	historical := now.Add(-6 * time.Hour)
	if got := runtimeMetricTime(historical, true, now); !got.Equal(now) {
		t.Fatalf("replay metric time should use now, got %v want %v", got, now)
	}
}

func TestRuntimeMetricTimeKeepsRealtimeTimestamp(t *testing.T) {
	now := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)
	current := now.Add(-250 * time.Millisecond)
	if got := runtimeMetricTime(current, false, now); !got.Equal(current) {
		t.Fatalf("realtime metric time should keep original timestamp, got %v want %v", got, current)
	}
}

func TestRuntimeMetricTimeUsesNowWhenZero(t *testing.T) {
	now := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)
	if got := runtimeMetricTime(time.Time{}, false, now); !got.Equal(now) {
		t.Fatalf("zero timestamp should fall back to now, got %v want %v", got, now)
	}
}
