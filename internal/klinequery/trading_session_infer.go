package klinequery

import (
	"database/sql"
	"sort"
	"time"

	"ctp-go-demo/internal/logger"
)

func tryInferTradingSessionFromL9Bars(db *sql.DB, variety string, bars []KlineBar) error {
	logger.Info("kline pipeline", "stage", "session_infer_begin", "variety", variety, "bars", len(bars))
	dayMinutes := make(map[string]map[int]struct{})
	for _, bar := range bars {
		dt := barDataTime(bar)
		if dt.IsZero() {
			continue
		}
		day := dt.Format("2006-01-02")
		minute := dt.Hour()*60 + dt.Minute()
		minute = floorMinuteTo5(minute)
		buckets := dayMinutes[day]
		if buckets == nil {
			buckets = make(map[int]struct{})
			dayMinutes[day] = buckets
		}
		buckets[minute] = struct{}{}
	}
	if len(dayMinutes) == 0 {
		logger.Info("kline pipeline", "stage", "session_infer_check", "variety", variety, "missing", true, "reason", "no_l9_1m_data")
		return nil
	}

	days := make([]string, 0, len(dayMinutes))
	for d := range dayMinutes {
		days = append(days, d)
	}
	sort.Strings(days)
	recent5 := pickRecent5TradingDays(days)
	middle3 := pickMiddle3OfRecent5(recent5)
	logger.Info("kline pipeline", "stage", "session_infer_days", "variety", variety, "recent5_count", len(recent5), "recent5_days", recent5, "middle3_days", middle3)
	if len(recent5) < 5 || len(middle3) < 3 {
		logger.Info("kline pipeline", "stage", "session_infer_check", "variety", variety, "missing", true, "reason", "infer_not_enough_days")
		return nil
	}

	dayA, dayB, dayC := middle3[0], middle3[1], middle3[2]
	setA := dayMinutes[dayA]
	setB := dayMinutes[dayB]
	setC := dayMinutes[dayC]
	if !minuteSetEqual(setA, setB) || !minuteSetEqual(setB, setC) {
		logger.Info("kline pipeline", "stage", "session_infer_check", "variety", variety, "missing", true, "reason", "infer_missing_data", "day_a", dayA, "day_b", dayB, "day_c", dayC)
		return nil
	}

	ranges := buildSessionRangesFromMinuteSet(setB)
	sessionText, sessionJSON, err := encodeSessionJSON(ranges)
	if err != nil {
		return err
	}
	rec := tradingSessionRecord{
		Variety:     variety,
		SessionText: sessionText,
		SessionJSON: sessionJSON,
		IsCompleted: true,
		MatchRatio:  1.0,
		UpdatedAt:   time.Now(),
	}
	if t, err := time.ParseInLocation("2006-01-02", dayA, time.Local); err == nil {
		rec.SampleDate = sql.NullTime{Valid: true, Time: t}
	}
	if t, err := time.ParseInLocation("2006-01-02", dayC, time.Local); err == nil {
		rec.ValidatedDate = sql.NullTime{Valid: true, Time: t}
	}
	logger.Info("kline pipeline", "stage", "session_infer_result", "variety", variety, "session_text", sessionText, "session_json", sessionJSON)
	return upsertTradingSession(db, rec)
}

func buildSessionRangesFromMinuteSet(set map[int]struct{}) []sessionMinuteRange {
	if len(set) == 0 {
		return nil
	}
	minutes := make([]int, 0, len(set))
	for m := range set {
		minutes = append(minutes, m)
	}
	sort.Slice(minutes, func(i, j int) bool {
		return tradingMinuteOrderKey(minutes[i]) < tradingMinuteOrderKey(minutes[j])
	})
	out := make([]sessionMinuteRange, 0, 8)
	start := minutes[0]
	prev := minutes[0]
	for i := 1; i < len(minutes); i += 1 {
		m := minutes[i]
		if m == prev+5 {
			prev = m
			continue
		}
		out = append(out, sessionMinuteRange{Start: start, End: prev})
		start = m
		prev = m
	}
	out = append(out, sessionMinuteRange{Start: start, End: prev})
	return out
}

func minuteSetEqual(a, b map[int]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func pickRecent5TradingDays(sortedAscDays []string) []string {
	if len(sortedAscDays) <= 5 {
		return append([]string(nil), sortedAscDays...)
	}
	return append([]string(nil), sortedAscDays[len(sortedAscDays)-5:]...)
}

func pickMiddle3OfRecent5(recent5 []string) []string {
	if len(recent5) < 5 {
		return nil
	}
	return append([]string(nil), recent5[1:4]...)
}

func floorMinuteTo5(minute int) int {
	if minute <= 0 {
		return 0
	}
	return (minute / 5) * 5
}

func barDataTime(bar KlineBar) time.Time {
	ts := bar.DataTime
	if ts <= 0 {
		ts = bar.AdjustedTime
	}
	if ts <= 0 {
		return time.Time{}
	}
	return time.Unix(ts, 0).In(time.Local)
}
