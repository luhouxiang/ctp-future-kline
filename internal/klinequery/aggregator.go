package klinequery

import (
	"database/sql"
	"fmt"
	"time"

	"ctp-go-demo/internal/logger"
)

func aggregateBarsByTimeframe(bars []KlineBar, timeframe string, timeframeMinutes int, sessions []sessionMinuteRange) []KlineBar {
	logger.Info("kline pipeline", "stage", "merge_begin", "timeframe", timeframe, "raw_1m_count", len(bars), "session_count", len(sessions))
	if timeframeMinutes <= 1 || len(bars) == 0 {
		return bars
	}
	slotIdx := buildSessionMinuteIndex(sessions)
	if len(slotIdx) == 0 {
		logger.Info("kline pipeline", "stage", "merge_abort", "error_class", "session_not_ready", "message", "session minute index is empty")
		return nil
	}

	type key struct {
		day    string
		bucket int
	}
	out := make([]KlineBar, 0, len(bars)/timeframeMinutes+8)
	pos := make(map[key]int, len(out))

	for _, bar := range bars {
		dt := barDataTime(bar)
		if dt.IsZero() {
			continue
		}
		minute := dt.Hour()*60 + dt.Minute()
		idx, ok := slotIdx[minute]
		if !ok {
			continue
		}
		k := key{
			day:    dt.Format("2006-01-02"),
			bucket: idx / timeframeMinutes,
		}
		p, exists := pos[k]
		if !exists {
			pos[k] = len(out)
			out = append(out, bar)
			continue
		}
		agg := out[p]
		if bar.High > agg.High {
			agg.High = bar.High
		}
		if bar.Low < agg.Low {
			agg.Low = bar.Low
		}
		agg.Close = bar.Close
		agg.Volume += bar.Volume
		agg.OpenInterest = bar.OpenInterest
		agg.AdjustedTime = bar.AdjustedTime
		agg.DataTime = bar.DataTime
		out[p] = agg
	}

	if len(out) == 0 {
		logger.Info("kline pipeline", "stage", "merge_abort", "error_class", "infer_missing_data", "message", "merged result is empty")
		return out
	}
	first := out[0]
	last := out[len(out)-1]
	logger.Info("kline pipeline",
		"stage", "merge_result",
		"timeframe", timeframe,
		"merged_count", len(out),
		"first_bar_time", formatBarUnix(first.AdjustedTime),
		"first_open", first.Open,
		"first_high", first.High,
		"first_low", first.Low,
		"first_close", first.Close,
		"last_bar_time", formatBarUnix(last.AdjustedTime),
		"last_open", last.Open,
		"last_high", last.High,
		"last_low", last.Low,
		"last_close", last.Close,
	)
	return out
}

func buildSessionMinuteIndex(sessions []sessionMinuteRange) map[int]int {
	out := make(map[int]int, 512)
	seq := 0
	for _, s := range sessions {
		if s.End < s.Start {
			continue
		}
		for m := s.Start; m <= s.End; m += 1 {
			if _, ok := out[m]; ok {
				continue
			}
			out[m] = seq
			seq += 1
		}
	}
	return out
}

func ensureCompletedTradingSession(db *sql.DB, variety string) ([]sessionMinuteRange, error) {
	rec, ok, err := loadTradingSession(db, variety)
	if err != nil {
		return nil, err
	}
	if !ok || !rec.IsCompleted {
		return nil, newTradingSessionNotReadyError()
	}
	ranges, err := decodeSessionJSON(rec.SessionJSON)
	if err != nil {
		return nil, fmt.Errorf("parse trading session for variety=%s failed: %w", variety, err)
	}
	logger.Info("kline pipeline", "stage", "session_ready", "variety", variety, "session_text", rec.SessionText, "session_count", len(ranges))
	return ranges, nil
}

func formatBarUnix(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.Unix(ts, 0).In(time.Local).Format("2006-01-02 15:04:05")
}
