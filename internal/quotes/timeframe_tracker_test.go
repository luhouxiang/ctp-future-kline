package quotes

import (
	"testing"
	"time"

	"ctp-future-kline/internal/sessiontime"
)

func TestTimeframeTrackerBuildsPartialAndFinalizesFiveMinute(t *testing.T) {
	t.Parallel()

	tracker := newTimeframeTracker()
	sessions := []sessiontime.Range{{Start: 9 * 60, End: 15 * 60}}

	minutes := []minuteBar{
		testMinuteBar("rb2405", "rb", "SHFE", "2026-02-10 09:01:00", "2026-02-10 09:01:00", 100, 101, 99, 100, 3),
		testMinuteBar("rb2405", "rb", "SHFE", "2026-02-10 09:02:00", "2026-02-10 09:02:00", 100, 102, 100, 101, 4),
		testMinuteBar("rb2405", "rb", "SHFE", "2026-02-10 09:03:00", "2026-02-10 09:03:00", 101, 103, 100, 102, 5),
	}
	tracker.RestoreFinals(minutes, sessions)

	partial, ok := findTrackedBar(tracker.BuildPartials(testMinuteBar("rb2405", "rb", "SHFE", "2026-02-10 09:04:00", "2026-02-10 09:04:00", 102, 104, 101, 103, 6), sessions), "5m")
	if !ok {
		t.Fatal("missing 5m partial")
	}
	if partial.Open != 100 || partial.Close != 103 || partial.High != 104 || partial.Low != 99 {
		t.Fatalf("unexpected 5m partial ohlc: %+v", partial)
	}
	if partial.Volume != 18 {
		t.Fatalf("partial volume=%d want 18", partial.Volume)
	}

	_, _ = tracker.ConsumeFinal(testMinuteBar("rb2405", "rb", "SHFE", "2026-02-10 09:04:00", "2026-02-10 09:04:00", 102, 104, 101, 103, 6), sessions)
	finals, _ := tracker.ConsumeFinal(testMinuteBar("rb2405", "rb", "SHFE", "2026-02-10 09:05:00", "2026-02-10 09:05:00", 103, 105, 102, 104, 7), sessions)
	final, ok := findTrackedBar(finals, "5m")
	if !ok {
		t.Fatal("missing 5m final")
	}
	if got := final.MinuteTime.Format("2006-01-02 15:04:05"); got != "2026-02-10 09:05:00" {
		t.Fatalf("5m final data_time=%s want 2026-02-10 09:05:00", got)
	}
	if final.Volume != 25 {
		t.Fatalf("final volume=%d want 25", final.Volume)
	}
}

func TestTimeframeTrackerDailyUsesTradingDayLabel(t *testing.T) {
	t.Parallel()

	tracker := newTimeframeTracker()
	sessions := []sessiontime.Range{
		{Start: 21 * 60, End: 23*60 + 59},
		{Start: 9 * 60, End: 15 * 60},
	}

	tracker.RestoreFinals([]minuteBar{
		testMinuteBar("ag2605", "ag", "SHFE", "2026-02-10 21:01:00", "2026-02-09 21:01:00", 100, 101, 99, 100, 3),
	}, sessions)
	partial, ok := findTrackedBar(tracker.BuildPartials(testMinuteBar("ag2605", "ag", "SHFE", "2026-02-10 09:01:00", "2026-02-10 09:01:00", 100, 102, 98, 101, 4), sessions), "1d")
	if !ok {
		t.Fatal("missing 1d partial")
	}
	if got := partial.MinuteTime.Format("2006-01-02 15:04:05"); got != "2026-02-10 21:00:00" {
		t.Fatalf("1d data_time=%s want 2026-02-10 21:00:00", got)
	}
	if got := partial.AdjustedTime.Format("2006-01-02 15:04:05"); got != "2026-02-09 21:00:00" {
		t.Fatalf("1d adjusted_time=%s want 2026-02-09 21:00:00", got)
	}
}

func testMinuteBar(instrumentID, variety, exchange, dataTime, adjustedTime string, open, high, low, close float64, volume int64) minuteBar {
	return minuteBar{
		Variety:          variety,
		InstrumentID:     instrumentID,
		Exchange:         exchange,
		MinuteTime:       mustTrackerTime(dataTime),
		AdjustedTime:     mustTrackerTime(adjustedTime),
		SourceReceivedAt: mustTrackerTime(adjustedTime),
		Period:           "1m",
		Open:             open,
		High:             high,
		Low:              low,
		Close:            close,
		Volume:           volume,
		OpenInterest:     100,
		SettlementPrice:  close,
	}
}

func mustTrackerTime(v string) time.Time {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", v, time.Local)
	if err != nil {
		panic(err)
	}
	return ts
}

func findTrackedBar(bars []minuteBar, period string) (minuteBar, bool) {
	for _, bar := range bars {
		if bar.Period == period {
			return bar, true
		}
	}
	return minuteBar{}, false
}
