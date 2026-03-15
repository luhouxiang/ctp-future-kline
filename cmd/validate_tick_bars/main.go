package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	dbx "ctp-go-demo/internal/db"
	"ctp-go-demo/internal/klineclock"
	"ctp-go-demo/internal/sessiontime"
)

type tickRow struct {
	InstrumentID   string
	ExchangeID     string
	TradingDay     string
	ActionDay      string
	UpdateTime     string
	UpdateMillisec int
	LastPrice      float64
	Volume         int
	OpenInterest   float64
	Settlement     float64
}

type bar struct {
	InstrumentID    string
	Exchange        string
	DataTime        time.Time
	AdjustedTime    time.Time
	Open            float64
	High            float64
	Low             float64
	Close           float64
	Volume          int64
	OpenInterest    float64
	SettlementPrice float64
}

type barState struct {
	bar            bar
	prevCloseVol   int
	hasPrev        bool
	lastTickVolume int
}

func main() {
	input := flag.String("input", "", "tick csv path")
	sessionText := flag.String("session", "", "session text like 21:00-02:30,09:00-15:00")
	dbPath := flag.String("db", "", "optional database path for session lookup and diff")
	variety := flag.String("variety", "", "variety for session lookup and db diff")
	instrument := flag.String("instrument", "", "instrument id filter")
	showLimit := flag.Int("show", 8, "number of bars to print")
	flag.Parse()

	if strings.TrimSpace(*input) == "" {
		fmt.Fprintln(os.Stderr, "-input is required")
		os.Exit(2)
	}

	var db *sql.DB
	var err error
	if strings.TrimSpace(*dbPath) != "" {
		db, err = dbx.Open(*dbPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open db failed: %v\n", err)
			os.Exit(1)
		}
		defer db.Close()
	}

	sessions, err := loadSessions(*sessionText, *variety, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load sessions failed: %v\n", err)
		os.Exit(1)
	}

	clock := klineclock.NewCalendarResolver(db)
	bars, err := aggregateCSV(*input, strings.TrimSpace(*instrument), sessions, clock)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aggregate csv failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("bars=%d\n", len(bars))
	limit := *showLimit
	if limit <= 0 || limit > len(bars) {
		limit = len(bars)
	}
	for i := 0; i < limit; i += 1 {
		b := bars[i]
		fmt.Printf("%s | data=%s adjusted=%s O=%.0f H=%.0f L=%.0f C=%.0f V=%d OI=%.0f\n",
			b.InstrumentID,
			b.DataTime.Format("2006-01-02 15:04:00"),
			b.AdjustedTime.Format("2006-01-02 15:04:00"),
			b.Open, b.High, b.Low, b.Close, b.Volume, b.OpenInterest,
		)
	}

	if db != nil && strings.TrimSpace(*variety) != "" {
		if err := compareWithDB(db, *variety, bars); err != nil {
			fmt.Fprintf(os.Stderr, "db compare failed: %v\n", err)
			os.Exit(1)
		}
	}
}

func aggregateCSV(path string, instrumentFilter string, sessions []sessiontime.Range, clock *klineclock.CalendarResolver) ([]bar, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	index := make(map[string]int, len(header))
	for i, name := range header {
		index[strings.TrimSpace(name)] = i
	}

	states := make(map[string]*barState)
	out := make([]bar, 0, 256)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		row, err := parseTickRecord(record, index)
		if err != nil {
			return nil, err
		}
		if instrumentFilter != "" && !strings.EqualFold(row.InstrumentID, instrumentFilter) {
			continue
		}
		dataTime, adjustedTime, err := resolveBarTimes(row, sessions, clock)
		if err != nil {
			return nil, err
		}
		state := states[row.InstrumentID]
		if state == nil || !state.bar.AdjustedTime.Equal(adjustedTime) {
			if state != nil {
				out = append(out, state.bar)
				state.prevCloseVol = state.lastTickVolume
				state.hasPrev = true
			} else {
				state = &barState{}
			}
			next := bar{
				InstrumentID:    row.InstrumentID,
				Exchange:        row.ExchangeID,
				DataTime:        dataTime,
				AdjustedTime:    adjustedTime,
				Open:            row.LastPrice,
				High:            row.LastPrice,
				Low:             row.LastPrice,
				Close:           row.LastPrice,
				Volume:          calcVolume(row.Volume, state.prevCloseVol, state.hasPrev),
				OpenInterest:    row.OpenInterest,
				SettlementPrice: row.Settlement,
			}
			state.lastTickVolume = row.Volume
			state.bar = next
			states[row.InstrumentID] = state
			continue
		}
		if row.LastPrice > state.bar.High {
			state.bar.High = row.LastPrice
		}
		if row.LastPrice < state.bar.Low {
			state.bar.Low = row.LastPrice
		}
		state.bar.Close = row.LastPrice
		state.bar.OpenInterest = row.OpenInterest
		state.bar.SettlementPrice = row.Settlement
		state.bar.Volume = calcVolume(row.Volume, state.prevCloseVol, state.hasPrev)
		state.lastTickVolume = row.Volume
	}
	for _, state := range states {
		out = append(out, state.bar)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AdjustedTime.Equal(out[j].AdjustedTime) {
			return out[i].InstrumentID < out[j].InstrumentID
		}
		return out[i].AdjustedTime.Before(out[j].AdjustedTime)
	})
	return out, nil
}

func parseTickRecord(record []string, index map[string]int) (tickRow, error) {
	get := func(name string) string {
		idx, ok := index[name]
		if !ok || idx >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[idx])
	}
	lastPrice, err := strconv.ParseFloat(get("last_price"), 64)
	if err != nil {
		return tickRow{}, fmt.Errorf("last_price: %w", err)
	}
	volume, err := strconv.Atoi(get("volume"))
	if err != nil {
		return tickRow{}, fmt.Errorf("volume: %w", err)
	}
	openInterest, err := strconv.ParseFloat(get("open_interest"), 64)
	if err != nil {
		return tickRow{}, fmt.Errorf("open_interest: %w", err)
	}
	updateMillisec := 0
	if raw := get("update_millisec"); raw != "" {
		updateMillisec, err = strconv.Atoi(raw)
		if err != nil {
			return tickRow{}, fmt.Errorf("update_millisec: %w", err)
		}
	}
	settlement := 0.0
	if raw := get("settlement_price"); raw != "" {
		settlement, err = strconv.ParseFloat(raw, 64)
		if err != nil {
			return tickRow{}, fmt.Errorf("settlement_price: %w", err)
		}
	}
	return tickRow{
		InstrumentID:   get("instrument_id"),
		ExchangeID:     get("exchange_id"),
		TradingDay:     get("trading_day"),
		ActionDay:      get("action_day"),
		UpdateTime:     get("update_time"),
		UpdateMillisec: updateMillisec,
		LastPrice:      lastPrice,
		Volume:         volume,
		OpenInterest:   openInterest,
		Settlement:     settlement,
	}, nil
}

func resolveBarTimes(row tickRow, sessions []sessiontime.Range, clock *klineclock.CalendarResolver) (time.Time, time.Time, error) {
	tradingDay, err := klineclock.ParseTradingDay(row.TradingDay)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	updateClock, err := time.ParseInLocation("15:04:05", row.UpdateTime, time.Local)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	hhmm := klineclock.HHMMFromTime(updateClock)
	_, actualMinute, err := klineclock.BuildBarTimes(tradingDay, hhmm, clock)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	labelMinute, ok := sessiontime.LabelMinute(updateClock.Hour()*60+updateClock.Minute(), sessions)
	if !ok {
		labelMinute = updateClock.Hour()*60 + updateClock.Minute()
		if labelMinute < 23*60+59 {
			labelMinute += 1
		}
	}
	dataTime := minuteOnDay(tradingDay, labelMinute)
	adjustedTime := minuteOnDay(actualMinute, labelMinute)
	return dataTime, adjustedTime, nil
}

func minuteOnDay(day time.Time, minuteOfDay int) time.Time {
	return time.Date(day.Year(), day.Month(), day.Day(), minuteOfDay/60, minuteOfDay%60, 0, 0, time.Local)
}

func calcVolume(current int, prev int, hasPrev bool) int64 {
	if !hasPrev {
		return 0
	}
	diff := current - prev
	if diff < 0 {
		return 0
	}
	return int64(diff)
}

func loadSessions(sessionText string, variety string, db *sql.DB) ([]sessiontime.Range, error) {
	if strings.TrimSpace(sessionText) != "" {
		return sessiontime.ParseSessionText(sessionText)
	}
	if db == nil || strings.TrimSpace(variety) == "" {
		return nil, fmt.Errorf("either -session or both -db/-variety are required")
	}
	var raw string
	var completed bool
	if err := db.QueryRow(`SELECT session_json,is_completed FROM trading_sessions WHERE variety=?`, strings.ToLower(strings.TrimSpace(variety))).Scan(&raw, &completed); err != nil {
		return nil, err
	}
	if !completed {
		return nil, fmt.Errorf("trading session not completed for %s", variety)
	}
	return sessiontime.DecodeSessionJSON(raw)
}

func compareWithDB(db *sql.DB, variety string, bars []bar) error {
	table := fmt.Sprintf("future_kline_instrument_1m_%s", strings.ToLower(strings.TrimSpace(variety)))
	rows, err := db.Query(fmt.Sprintf(`SELECT "InstrumentID","DataTime","AdjustedTime","Open","High","Low","Close","Volume","OpenInterest" FROM "%s" ORDER BY "AdjustedTime" ASC`, table))
	if err != nil {
		return err
	}
	defer rows.Close()
	type key struct {
		instrument string
		adjusted   string
	}
	dbBars := make(map[key]bar)
	for rows.Next() {
		var b bar
		if err := rows.Scan(&b.InstrumentID, &b.DataTime, &b.AdjustedTime, &b.Open, &b.High, &b.Low, &b.Close, &b.Volume, &b.OpenInterest); err != nil {
			return err
		}
		dbBars[key{instrument: b.InstrumentID, adjusted: b.AdjustedTime.Format("2006-01-02 15:04:00")}] = b
	}
	diffCount := 0
	for _, b := range bars {
		k := key{instrument: b.InstrumentID, adjusted: b.AdjustedTime.Format("2006-01-02 15:04:00")}
		old, ok := dbBars[k]
		if !ok {
			fmt.Printf("missing_in_db | %s | %s\n", b.InstrumentID, k.adjusted)
			diffCount += 1
			continue
		}
		if !sameBar(old, b) {
			fmt.Printf("diff | %s | adjusted=%s | data old=%s new=%s | OHLVC old=(%.0f,%.0f,%.0f,%.0f,%d) new=(%.0f,%.0f,%.0f,%.0f,%d)\n",
				b.InstrumentID, k.adjusted,
				old.DataTime.Format("2006-01-02 15:04:00"), b.DataTime.Format("2006-01-02 15:04:00"),
				old.Open, old.High, old.Low, old.Close, old.Volume,
				b.Open, b.High, b.Low, b.Close, b.Volume,
			)
			diffCount += 1
		}
	}
	fmt.Printf("db_diff_count=%d\n", diffCount)
	return rows.Err()
}

func sameBar(a bar, b bar) bool {
	return a.DataTime.Equal(b.DataTime) &&
		a.AdjustedTime.Equal(b.AdjustedTime) &&
		a.Open == b.Open &&
		a.High == b.High &&
		a.Low == b.Low &&
		a.Close == b.Close &&
		a.Volume == b.Volume &&
		a.OpenInterest == b.OpenInterest
}
