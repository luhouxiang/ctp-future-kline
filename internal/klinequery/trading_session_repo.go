package klinequery

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"ctp-future-kline/internal/logger"
)

type tradingSessionRange struct {
	// Start 是 JSON 中的起始 HH:MM。
	Start string `json:"start"`
	// End 是 JSON 中的结束 HH:MM。
	End string `json:"end"`
}

type sessionMinuteRange struct {
	// Start 是交易时段起始分钟。
	Start int
	// End 是交易时段结束分钟。
	End int
}

type tradingSessionRecord struct {
	// Variety 是品种代码。
	Variety string
	// SessionText 是交易时段的文本表示。
	SessionText string
	// SessionJSON 是交易时段的结构化 JSON 表示。
	SessionJSON string
	// IsCompleted 表示该品种时段是否已经确认完整。
	IsCompleted bool
	// SampleDate 是用于推断时段的样本交易日。
	SampleDate sql.NullTime
	// ValidatedDate 是最后一次验证通过的交易日。
	ValidatedDate sql.NullTime
	// MatchRatio 是时段推断和实际数据匹配度。
	MatchRatio float64
	// UpdatedAt 是记录更新时间。
	UpdatedAt time.Time
}

func loadTradingSession(db *sql.DB, variety string) (tradingSessionRecord, bool, error) {
	v := strings.ToLower(strings.TrimSpace(variety))
	logger.Info("kline pipeline", "stage", "session_lookup", "variety", v)
	var rec tradingSessionRecord
	err := db.QueryRow(`
SELECT variety,session_text,session_json,is_completed,sample_trade_date,validated_trade_date,match_ratio,updated_at
FROM trading_sessions WHERE variety=?`, v).Scan(
		&rec.Variety,
		&rec.SessionText,
		&rec.SessionJSON,
		&rec.IsCompleted,
		&rec.SampleDate,
		&rec.ValidatedDate,
		&rec.MatchRatio,
		&rec.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Info("kline pipeline", "stage", "session_lookup", "variety", v, "found", false, "is_completed", false)
			return tradingSessionRecord{}, false, nil
		}
		return tradingSessionRecord{}, false, fmt.Errorf("query trading session failed: %w", err)
	}
	logger.Info("kline pipeline", "stage", "session_lookup", "variety", v, "found", true, "is_completed", rec.IsCompleted, "match_ratio", rec.MatchRatio, "updated_at", rec.UpdatedAt.Format("2006-01-02 15:04:05"))
	return rec, true, nil
}

func upsertTradingSession(db *sql.DB, rec tradingSessionRecord) error {
	v := strings.ToLower(strings.TrimSpace(rec.Variety))
	_, err := db.Exec(`
INSERT INTO trading_sessions(variety,session_text,session_json,is_completed,sample_trade_date,validated_trade_date,match_ratio,updated_at)
VALUES(?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
  session_text=VALUES(session_text),
  session_json=VALUES(session_json),
  is_completed=VALUES(is_completed),
  sample_trade_date=VALUES(sample_trade_date),
  validated_trade_date=VALUES(validated_trade_date),
  match_ratio=VALUES(match_ratio),
  updated_at=VALUES(updated_at)
`,
		v,
		rec.SessionText,
		rec.SessionJSON,
		rec.IsCompleted,
		nullTimeToDate(rec.SampleDate),
		nullTimeToDate(rec.ValidatedDate),
		rec.MatchRatio,
		rec.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert trading session failed: %w", err)
	}
	logger.Info("kline pipeline", "stage", "session_upsert", "variety", v, "is_completed", rec.IsCompleted, "match_ratio", rec.MatchRatio, "session_text", rec.SessionText)
	return nil
}

func nullTimeToDate(v sql.NullTime) any {
	if !v.Valid {
		return nil
	}
	return v.Time.Format("2006-01-02")
}

func encodeSessionJSON(ranges []sessionMinuteRange) (string, string, error) {
	merged := mergeDisplayRanges(ranges)
	out := make([]tradingSessionRange, 0, len(merged))
	texts := make([]string, 0, len(merged))
	for _, r := range merged {
		if r.End < r.Start {
			start := minuteToHHMM(r.Start)
			end := minuteToHHMM(r.End)
			out = append(out, tradingSessionRange{Start: start, End: end})
			texts = append(texts, start+"-"+end)
			continue
		}
		start := minuteToHHMM(r.Start)
		end := minuteToHHMM(r.End)
		out = append(out, tradingSessionRange{Start: start, End: end})
		texts = append(texts, start+"-"+end)
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return "", "", err
	}
	return strings.Join(texts, ","), string(raw), nil
}

func mergeDisplayRanges(ranges []sessionMinuteRange) []sessionMinuteRange {
	if len(ranges) <= 1 {
		return ranges
	}
	out := make([]sessionMinuteRange, 0, len(ranges))
	i := 0
	for i < len(ranges) {
		cur := ranges[i]
		if i+1 < len(ranges) {
			next := ranges[i+1]
			if cur.End >= 23*60+55 && next.Start == 0 {
				out = append(out, sessionMinuteRange{Start: cur.Start, End: next.End})
				i += 2
				continue
			}
		}
		out = append(out, cur)
		i++
	}
	return out
}

func decodeSessionJSON(raw string) ([]sessionMinuteRange, error) {
	var src []tradingSessionRange
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	if err := json.Unmarshal([]byte(raw), &src); err != nil {
		return nil, fmt.Errorf("decode session_json failed: %w", err)
	}
	out := make([]sessionMinuteRange, 0, len(src))
	for _, r := range src {
		start, err := parseHHMM(r.Start)
		if err != nil {
			return nil, err
		}
		end, err := parseHHMM(r.End)
		if err != nil {
			return nil, err
		}
		// Support cross-midnight session like 21:00-02:30.
		if end < start {
			out = append(out, sessionMinuteRange{Start: start, End: 23*60 + 59})
			out = append(out, sessionMinuteRange{Start: 0, End: end})
			continue
		}
		out = append(out, sessionMinuteRange{Start: start, End: end})
	}
	sort.Slice(out, func(i, j int) bool {
		return tradingMinuteOrderKey(out[i].Start) < tradingMinuteOrderKey(out[j].Start)
	})
	return out, nil
}

func parseHHMM(v string) (int, error) {
	s := strings.TrimSpace(v)
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid HH:MM: %s", v)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("invalid hour in %s", v)
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("invalid minute in %s", v)
	}
	if h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, fmt.Errorf("invalid HH:MM: %s", v)
	}
	return h*60 + m, nil
}

func minuteToHHMM(minute int) string {
	if minute < 0 {
		minute = 0
	}
	h := (minute / 60) % 24
	m := minute % 60
	return fmt.Sprintf("%02d:%02d", h, m)
}

func tradingMinuteOrderKey(minute int) int {
	// Night session comes first in a trading day sequence.
	if minute >= 18*60 {
		return minute
	}
	return minute + 24*60
}
