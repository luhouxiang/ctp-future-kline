package quotes

import (
	"testing"
	"time"

	"ctp-future-kline/internal/sessiontime"
)

func TestReplayCleanupCurrentAdjustedUsesSubscribedTimeframeLabel(t *testing.T) {
	sessions := sessiontime.DefaultRanges()
	base := time.Date(2026, 4, 29, 21, 3, 0, 0, time.Local)
	adjusted := time.Date(2026, 4, 29, 21, 3, 0, 0, time.Local)

	got, ok := replayCleanupCurrentAdjusted(base, adjusted, "1m", sessions)
	if !ok || got.Format("15:04") != "21:03" {
		t.Fatalf("1m current adjusted = %v ok=%v, want 21:03 true", got, ok)
	}

	got, ok = replayCleanupCurrentAdjusted(base, adjusted, "5m", sessions)
	if !ok || got.Format("15:04") != "21:05" {
		t.Fatalf("5m current adjusted = %v ok=%v, want 21:05 true", got, ok)
	}
}
