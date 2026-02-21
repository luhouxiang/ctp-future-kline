package klineclock

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"ctp-go-demo/internal/logger"
)

const dayLayout = "2006-01-02"

// PrevTradingDayProvider returns previous trading day for a given trading day.
type PrevTradingDayProvider interface {
	PrevTradingDay(day time.Time) (time.Time, error)
}

// CalendarResolver resolves previous trading day from sqlite trading_calendar with fallback.
type CalendarResolver struct {
	db *sql.DB

	mu                  sync.Mutex
	cache               map[string]time.Time
	warnedMissingTable  bool
	warnedMissingRecord bool
}

func NewCalendarResolver(db *sql.DB) *CalendarResolver {
	return &CalendarResolver{
		db:    db,
		cache: make(map[string]time.Time),
	}
}

func (r *CalendarResolver) PrevTradingDay(day time.Time) (time.Time, error) {
	day = normalizeDay(day)
	key := day.Format(dayLayout)

	r.mu.Lock()
	if prev, ok := r.cache[key]; ok {
		r.mu.Unlock()
		return prev, nil
	}
	r.mu.Unlock()

	prev, err := r.queryPrevTradingDay(day)
	if err == nil {
		r.mu.Lock()
		r.cache[key] = prev
		r.mu.Unlock()
		return prev, nil
	}

	// fallback to weekday logic when calendar unavailable/incomplete
	fallback := prevWorkingDay(day)
	r.mu.Lock()
	r.cache[key] = fallback
	missingTable := isMissingTableErr(err)
	if missingTable && !r.warnedMissingTable {
		logger.Info("trading calendar missing, fallback to weekday rule", "error", err)
		r.warnedMissingTable = true
	} else if !missingTable && !r.warnedMissingRecord {
		logger.Info("trading calendar has no previous trading day, fallback to weekday rule", "day", key, "error", err)
		r.warnedMissingRecord = true
	}
	r.mu.Unlock()
	return fallback, nil
}

func (r *CalendarResolver) queryPrevTradingDay(day time.Time) (time.Time, error) {
	if r == nil || r.db == nil {
		return time.Time{}, fmt.Errorf("nil calendar db")
	}
	var raw any
	err := r.db.QueryRow(
		`SELECT trade_date FROM trading_calendar WHERE is_open=1 AND trade_date < ? ORDER BY trade_date DESC LIMIT 1`,
		day.Format(dayLayout),
	).Scan(&raw)
	if err != nil {
		return time.Time{}, err
	}
	out, err := parseTradeDate(raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse trade_date failed: %w", err)
	}
	return normalizeDay(out), nil
}

func parseTradeDate(raw any) (time.Time, error) {
	switch v := raw.(type) {
	case time.Time:
		return v, nil
	case []byte:
		return parseTradeDateString(string(v))
	case string:
		return parseTradeDateString(v)
	default:
		return time.Time{}, fmt.Errorf("unsupported trade_date type %T", raw)
	}
}

func parseTradeDateString(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty trade_date")
	}
	layouts := []string{
		dayLayout,
		"2006-01-02 15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported trade_date value %q", s)
}

func BuildBarTimes(tradingDay time.Time, hhmm int, provider PrevTradingDayProvider) (time.Time, time.Time, error) {
	if hhmm < 0 || hhmm > 2359 {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid hhmm: %d", hhmm)
	}
	hour := hhmm / 100
	minute := hhmm % 100
	if hour > 23 || minute > 59 {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid hhmm: %d", hhmm)
	}

	tradingDay = normalizeDay(tradingDay)
	base := time.Date(tradingDay.Year(), tradingDay.Month(), tradingDay.Day(), hour, minute, 0, 0, time.Local)

	// day session keeps same timestamp.
	if hhmm >= 800 && hhmm <= 1600 {
		return base, base, nil
	}

	var prev time.Time
	var err error
	if provider != nil {
		prev, err = provider.PrevTradingDay(tradingDay)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
	} else {
		prev = prevWorkingDay(tradingDay)
	}

	adjDate := prev
	if hhmm < 800 {
		adjDate = prev.AddDate(0, 0, 1)
	}
	adjusted := time.Date(adjDate.Year(), adjDate.Month(), adjDate.Day(), hour, minute, 0, 0, time.Local)
	return base, adjusted, nil
}

func HHMMFromTime(ts time.Time) int {
	return ts.Hour()*100 + ts.Minute()
}

func ParseTradingDay(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if len(value) != 8 {
		return time.Time{}, fmt.Errorf("invalid trading day: %q", value)
	}
	out, err := time.ParseInLocation("20060102", value, time.Local)
	if err != nil {
		return time.Time{}, err
	}
	return normalizeDay(out), nil
}

func normalizeDay(day time.Time) time.Time {
	return time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.Local)
}

func prevWorkingDay(day time.Time) time.Time {
	d := normalizeDay(day).AddDate(0, 0, -1)
	switch d.Weekday() {
	case time.Saturday:
		return d.AddDate(0, 0, -1)
	case time.Sunday:
		return d.AddDate(0, 0, -2)
	default:
		return d
	}
}

func isMissingTableErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such table") ||
		strings.Contains(msg, "doesn't exist")
}
