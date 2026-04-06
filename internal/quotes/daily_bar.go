package quotes

import (
	"time"

	"ctp-future-kline/internal/sessiontime"
)

func tradingDayKey(bar minuteBar) string {
	if bar.MinuteTime.IsZero() {
		return ""
	}
	return bar.MinuteTime.Format("2006-01-02")
}

func dailySessionStartMinute(sessions []sessiontime.Range) int {
	if len(sessions) == 0 {
		return 0
	}
	sorted := sessiontime.SortRangesCopy(sessions)
	start := sorted[0].Start
	if start < 0 {
		return 0
	}
	return start
}

func dailyLabelTimes(bar minuteBar, sessions []sessiontime.Range) (time.Time, time.Time, bool) {
	if bar.MinuteTime.IsZero() {
		return time.Time{}, time.Time{}, false
	}
	start := dailySessionStartMinute(sessions)
	dataLabel := time.Date(
		bar.MinuteTime.Year(),
		bar.MinuteTime.Month(),
		bar.MinuteTime.Day(),
		start/60,
		start%60,
		0,
		0,
		time.Local,
	)
	adjustedBase := chooseAdjustedTime(bar)
	adjustedLabel := time.Date(
		adjustedBase.Year(),
		adjustedBase.Month(),
		adjustedBase.Day(),
		start/60,
		start%60,
		0,
		0,
		time.Local,
	)
	return dataLabel, adjustedLabel, true
}
