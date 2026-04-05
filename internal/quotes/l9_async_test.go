package quotes

import (
	"testing"
	"time"
)

func TestSnapshotBarsForMinuteDoesNotBackfillMissingContracts(t *testing.T) {
	t.Parallel()

	c := &l9AsyncCalculator{
		expected: map[string]map[string]struct{}{
			"ag": {
				"ag2605": {},
				"ag2606": {},
			},
		},
		latest:            make(map[string]minuteBar),
		minuteBars:        make(map[string]map[int64]map[string]minuteBar),
		instrumentVariety: make(map[string]string),
		aggregators:       make(map[string]*closedBarAggregator),
	}
	minuteTime := time.Date(2026, 4, 5, 21, 1, 0, 0, time.Local)
	c.ObserveMinuteBar(minuteBar{
		Variety:      "ag",
		InstrumentID: "ag2605",
		MinuteTime:   minuteTime,
		AdjustedTime: minuteTime,
		Period:       "1m",
		Open:         1,
		High:         2,
		Low:          1,
		Close:        2,
		Volume:       10,
		OpenInterest: 20,
	})

	bars := c.snapshotBarsForMinute("ag", minuteTime)
	if len(bars) != 1 {
		t.Fatalf("snapshotBarsForMinute() len=%d want 1", len(bars))
	}
	if bars[0].InstrumentID != "ag2605" {
		t.Fatalf("snapshotBarsForMinute() instrument=%s want ag2605", bars[0].InstrumentID)
	}
}
