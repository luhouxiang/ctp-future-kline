package trader_test

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"ctp-go-demo/tests/internal/trader/testkit"
)

func TestL9AsyncCalculatorComputesAndStores(t *testing.T) {
	t.Parallel()

	store := newTempStore(t)
	minute := mustMinute("2026-02-07 09:30:00")

	calc := testkit.NewL9AsyncCalculator(store, true, 1, map[string][]string{
		"rb": {"rb2405", "rb2410"},
	})
	t.Cleanup(calc.Close)

	calc.ObserveMinuteBar(testkit.MinuteBar{
		Variety:         "rb",
		InstrumentID:    "rb2405",
		Exchange:        "SHFE",
		MinuteTime:      minute,
		Period:          "1m",
		Open:            100,
		High:            110,
		Low:             95,
		Close:           105,
		Volume:          10,
		OpenInterest:    200,
		SettlementPrice: 101,
	})
	calc.ObserveMinuteBar(testkit.MinuteBar{
		Variety:         "rb",
		InstrumentID:    "rb2410",
		Exchange:        "SHFE",
		MinuteTime:      minute,
		Period:          "1m",
		Open:            200,
		High:            220,
		Low:             190,
		Close:           210,
		Volume:          20,
		OpenInterest:    100,
		SettlementPrice: 201,
	})

	calc.Submit("rb", minute)

	bar := queryL9BarEventually(t, store, "rb", minute)
	if bar.InstrumentID != "l9" {
		t.Fatalf("l9 instrument_id = %q, want %q", bar.InstrumentID, "l9")
	}
	if bar.Exchange != "L9" {
		t.Fatalf("l9 exchange = %q, want %q", bar.Exchange, "L9")
	}
	if !almostEqual(bar.Open, (100.0*200.0+200.0*100.0)/300.0) {
		t.Fatalf("l9 open = %v", bar.Open)
	}
	if !almostEqual(bar.High, (110.0*200.0+220.0*100.0)/300.0) {
		t.Fatalf("l9 high = %v", bar.High)
	}
	if !almostEqual(bar.Low, (95.0*200.0+190.0*100.0)/300.0) {
		t.Fatalf("l9 low = %v", bar.Low)
	}
	if !almostEqual(bar.Close, (105.0*200.0+210.0*100.0)/300.0) {
		t.Fatalf("l9 close = %v", bar.Close)
	}
	if bar.Volume != 30 {
		t.Fatalf("l9 volume = %d, want 30", bar.Volume)
	}
	if !almostEqual(bar.OpenInterest, 300.0) {
		t.Fatalf("l9 open_interest = %v, want 300", bar.OpenInterest)
	}
	if !almostEqual(bar.SettlementPrice, (101.0*200.0+201.0*100.0)/300.0) {
		t.Fatalf("l9 settlement = %v", bar.SettlementPrice)
	}
}

func TestL9AsyncCalculatorFallbacksToLatestWhenMissingCurrentMinute(t *testing.T) {
	t.Parallel()

	store := newTempStore(t)
	lastMinute := mustMinute("2026-02-07 09:29:00")
	minute := mustMinute("2026-02-07 09:30:00")

	calc := testkit.NewL9AsyncCalculator(store, true, 1, map[string][]string{
		"rb": {"rb2405", "rb2410"},
	})
	t.Cleanup(calc.Close)

	calc.ObserveMinuteBar(testkit.MinuteBar{
		Variety:         "rb",
		InstrumentID:    "rb2410",
		Exchange:        "SHFE",
		MinuteTime:      lastMinute,
		Period:          "1m",
		Open:            198,
		High:            202,
		Low:             197,
		Close:           200,
		Volume:          12,
		OpenInterest:    100,
		SettlementPrice: 201,
	})
	calc.ObserveMinuteBar(testkit.MinuteBar{
		Variety:         "rb",
		InstrumentID:    "rb2405",
		Exchange:        "SHFE",
		MinuteTime:      minute,
		Period:          "1m",
		Open:            100,
		High:            110,
		Low:             90,
		Close:           105,
		Volume:          10,
		OpenInterest:    200,
		SettlementPrice: 101,
	})

	calc.Submit("rb", minute)

	bar := queryL9BarEventually(t, store, "rb", minute)
	if bar.Volume != 10 {
		t.Fatalf("l9 volume = %d, want 10 (missing contract contributes 0)", bar.Volume)
	}
	if !almostEqual(bar.Open, (100.0*200.0+200.0*100.0)/300.0) {
		t.Fatalf("l9 open with fallback = %v", bar.Open)
	}
	if !almostEqual(bar.High, (110.0*200.0+200.0*100.0)/300.0) {
		t.Fatalf("l9 high with fallback = %v", bar.High)
	}
	if !almostEqual(bar.Low, (90.0*200.0+200.0*100.0)/300.0) {
		t.Fatalf("l9 low with fallback = %v", bar.Low)
	}
	if !almostEqual(bar.Close, (105.0*200.0+200.0*100.0)/300.0) {
		t.Fatalf("l9 close with fallback = %v", bar.Close)
	}
}

func TestL9AsyncCalculatorDisable(t *testing.T) {
	t.Parallel()

	store := newTempStore(t)
	minute := mustMinute("2026-02-07 09:30:00")

	calc := testkit.NewL9AsyncCalculator(store, true, 1, map[string][]string{"rb": {"rb2405"}})
	t.Cleanup(calc.Close)
	calc.Disable()

	calc.ObserveMinuteBar(testkit.MinuteBar{
		Variety:         "rb",
		InstrumentID:    "rb2405",
		Exchange:        "SHFE",
		MinuteTime:      minute,
		Period:          "1m",
		Open:            100,
		High:            110,
		Low:             95,
		Close:           105,
		Volume:          10,
		OpenInterest:    200,
		SettlementPrice: 101,
	})
	calc.Submit("rb", minute)

	time.Sleep(200 * time.Millisecond)
	if _, err := queryL9Bar(store, "rb", minute); err == nil {
		t.Fatal("l9 row exists while calculator disabled")
	}
}

func queryL9BarEventually(t *testing.T, store *testkit.KlineStore, variety string, minute time.Time) testkit.MinuteBar {
	t.Helper()
	var out testkit.MinuteBar
	assertEventually(t, 2*time.Second, func() bool {
		bar, err := queryL9Bar(store, variety, minute)
		if err != nil {
			return false
		}
		out = bar
		return true
	})
	return out
}

func queryL9Bar(store *testkit.KlineStore, variety string, minute time.Time) (testkit.MinuteBar, error) {
	tableName, err := testkit.TableNameForL9Variety(variety)
	if err != nil {
		return testkit.MinuteBar{}, err
	}
	query := fmt.Sprintf(`SELECT "%s","%s","%s","%s","%s","%s","%s","%s","%s" FROM "%s" WHERE "%s" = ? AND "%s" = ?`,
		testkit.ColInstrumentID, testkit.ColExchange, testkit.ColOpen, testkit.ColHigh, testkit.ColLow, testkit.ColClose, testkit.ColVolume, testkit.ColOpenInterest, testkit.ColSettlement,
		tableName,
		testkit.ColTime, testkit.ColPeriod,
	)
	row := store.DB().QueryRow(query, minute.Format("2006-01-02 15:04:00"), "1m")

	var bar testkit.MinuteBar
	if err := row.Scan(
		&bar.InstrumentID,
		&bar.Exchange,
		&bar.Open,
		&bar.High,
		&bar.Low,
		&bar.Close,
		&bar.Volume,
		&bar.OpenInterest,
		&bar.SettlementPrice,
	); err != nil {
		return testkit.MinuteBar{}, err
	}
	bar.Variety = testkit.NormalizeVariety(variety)
	bar.MinuteTime = minute
	bar.Period = "1m"
	return bar, nil
}

func newTempStore(t *testing.T) *testkit.KlineStore {
	t.Helper()
	store, err := testkit.NewKlineStore(filepath.Join(t.TempDir(), "kline.db"))
	if err != nil {
		t.Fatalf("newKlineStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func mustMinute(v string) time.Time {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", v, time.Local)
	if err != nil {
		panic(err)
	}
	return ts
}

func assertEventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func almostEqual(a float64, b float64) bool {
	return math.Abs(a-b) < 1e-9
}
