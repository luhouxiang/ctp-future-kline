package trader_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"ctp-go-demo/tests/internal/trader/testkit"
	ctp "github.com/kkqy/ctp-go"
)

func TestMdSpiAggregatesAndStoresOneMinuteKline(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	store, err := testkit.NewKlineStore(dbPath)
	if err != nil {
		t.Fatalf("newKlineStore() error = %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	spi := testkit.NewMdSpi(store, nil)

	pushTick(t, spi, tickInput{
		instrumentID: "rb2405",
		exchangeID:   "SHFE",
		actionDay:    "20260207",
		tradingDay:   "20260210",
		updateTime:   "09:30:01",
		lastPrice:    100,
		volume:       10,
		openInterest: 200,
		settlement:   99.0,
	})
	pushTick(t, spi, tickInput{
		instrumentID: "rb2405",
		exchangeID:   "SHFE",
		actionDay:    "20260207",
		tradingDay:   "20260210",
		updateTime:   "09:30:40",
		lastPrice:    102,
		volume:       14,
		openInterest: 201,
		settlement:   99.5,
	})
	pushTick(t, spi, tickInput{
		instrumentID: "rb2405",
		exchangeID:   "SHFE",
		actionDay:    "20260207",
		tradingDay:   "20260210",
		updateTime:   "09:31:05",
		lastPrice:    101,
		volume:       20,
		openInterest: 198,
		settlement:   100.0,
	})

	if err := spi.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	query := fmt.Sprintf(
		`SELECT "%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s" FROM "future_kline_instrument_1m_rb" ORDER BY "%s"`,
		testkit.ColInstrumentID, testkit.ColExchange, testkit.ColTime, testkit.ColAdjustedTime, testkit.ColPeriod, testkit.ColOpen, testkit.ColHigh, testkit.ColLow, testkit.ColClose, testkit.ColVolume, testkit.ColOpenInterest, testkit.ColSettlement, testkit.ColTime,
	)
	rows, err := store.DB().Query(query)
	if err != nil {
		t.Fatalf("query kline rows failed: %v", err)
	}
	defer rows.Close()

	type row struct {
		code       string
		exchange   string
		tm         string
		adjusted   string
		period     string
		open       float64
		high       float64
		low        float64
		close      float64
		volume     int64
		openInt    float64
		settlement float64
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.code, &r.exchange, &r.tm, &r.adjusted, &r.period, &r.open, &r.high, &r.low, &r.close, &r.volume, &r.openInt, &r.settlement); err != nil {
			t.Fatalf("scan kline row failed: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate kline rows failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("row count = %d, want 2", len(got))
	}

	if got[0].code != "2405" || got[0].exchange != "SHFE" || got[0].tm != "2026-02-10 09:30:00" || got[0].adjusted != "2026-02-10 09:30:00" || got[0].period != "1m" {
		t.Fatalf("first row meta = %+v, want code 2405, exchange SHFE, time+adjusted 2026-02-10 09:30:00, period 1m", got[0])
	}
	if got[0].open != 100 || got[0].high != 102 || got[0].low != 100 || got[0].close != 102 {
		t.Fatalf("first row ohlc = (%v,%v,%v,%v), want (100,102,100,102)", got[0].open, got[0].high, got[0].low, got[0].close)
	}
	if got[0].volume != 4 || got[0].openInt != 201 || got[0].settlement != 99.5 {
		t.Fatalf("first row vol/oi/settlement = (%d,%v,%v), want (4,201,99.5)", got[0].volume, got[0].openInt, got[0].settlement)
	}

	if got[1].code != "2405" || got[1].exchange != "SHFE" || got[1].tm != "2026-02-10 09:31:00" || got[1].adjusted != "2026-02-10 09:31:00" || got[1].period != "1m" {
		t.Fatalf("second row meta = %+v, want code 2405, exchange SHFE, time+adjusted 2026-02-10 09:31:00, period 1m", got[1])
	}
	if got[1].open != 101 || got[1].high != 101 || got[1].low != 101 || got[1].close != 101 {
		t.Fatalf("second row ohlc = (%v,%v,%v,%v), want (101,101,101,101)", got[1].open, got[1].high, got[1].low, got[1].close)
	}
	if got[1].volume != 6 || got[1].openInt != 198 || got[1].settlement != 100.0 {
		t.Fatalf("second row vol/oi/settlement = (%d,%v,%v), want (6,198,100)", got[1].volume, got[1].openInt, got[1].settlement)
	}
}

func TestTableNameForVariety(t *testing.T) {
	t.Parallel()

	name, err := testkit.TableNameForVariety("RB2405")
	if err != nil {
		t.Fatalf("tableNameForVariety() error = %v", err)
	}
	if name != "future_kline_instrument_1m_rb" {
		t.Fatalf("tableNameForVariety() = %q, want %q", name, "future_kline_instrument_1m_rb")
	}
}

func TestMdSpiDedupDuplicateTickWithinWindow(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	store, err := testkit.NewKlineStore(dbPath)
	if err != nil {
		t.Fatalf("newKlineStore() error = %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	spi := testkit.NewMdSpi(store, nil)

	pushTick(t, spi, tickInput{
		instrumentID: "rb2405",
		exchangeID:   "SHFE",
		actionDay:    "20260207",
		tradingDay:   "20260210",
		updateTime:   "09:30:01",
		lastPrice:    100,
		volume:       10,
		openInterest: 200,
		settlement:   99.0,
	})
	pushTick(t, spi, tickInput{
		instrumentID: "rb2405",
		exchangeID:   "SHFE",
		actionDay:    "20260207",
		tradingDay:   "20260210",
		updateTime:   "09:30:01",
		lastPrice:    100,
		volume:       10,
		openInterest: 200,
		settlement:   99.0,
	})
	pushTick(t, spi, tickInput{
		instrumentID: "rb2405",
		exchangeID:   "SHFE",
		actionDay:    "20260207",
		tradingDay:   "20260210",
		updateTime:   "09:31:05",
		lastPrice:    101,
		volume:       20,
		openInterest: 201,
		settlement:   99.5,
	})

	if err := spi.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	query := fmt.Sprintf(
		`SELECT "%s" FROM "future_kline_instrument_1m_rb" WHERE "%s" = ?`,
		testkit.ColVolume, testkit.ColTime,
	)
	row := store.DB().QueryRow(query, "2026-02-10 09:30:00")

	var gotVol int64
	if err := row.Scan(&gotVol); err != nil {
		t.Fatalf("scan volume failed: %v", err)
	}
	if gotVol != 0 {
		t.Fatalf("volume with duplicate tick = %d, want 0", gotVol)
	}
}

func TestMdSpiDriftPauseAndResume(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "kline.db")
	store, err := testkit.NewKlineStore(dbPath)
	if err != nil {
		t.Fatalf("newKlineStore() error = %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	spi := testkit.NewMdSpi(store, nil)

	now := time.Now()
	day := now.Format("20060102")
	late := now.Add(-30 * time.Second).Format("15:04:05")
	ok1 := now.Format("15:04:05")
	ok2 := now.Add(1 * time.Second).Format("15:04:05")
	ok3 := now.Add(2 * time.Second).Format("15:04:05")

	pushTick(t, spi, tickInput{
		instrumentID: "rb2405",
		exchangeID:   "SHFE",
		actionDay:    day,
		tradingDay:   day,
		updateTime:   late,
		lastPrice:    100,
		volume:       10,
		openInterest: 200,
		settlement:   99.0,
	})
	pushTick(t, spi, tickInput{
		instrumentID: "rb2405",
		exchangeID:   "SHFE",
		actionDay:    day,
		tradingDay:   day,
		updateTime:   ok1,
		lastPrice:    101,
		volume:       11,
		openInterest: 201,
		settlement:   99.1,
	})
	pushTick(t, spi, tickInput{
		instrumentID: "rb2405",
		exchangeID:   "SHFE",
		actionDay:    day,
		tradingDay:   day,
		updateTime:   ok2,
		lastPrice:    102,
		volume:       12,
		openInterest: 202,
		settlement:   99.2,
	})
	pushTick(t, spi, tickInput{
		instrumentID: "rb2405",
		exchangeID:   "SHFE",
		actionDay:    day,
		tradingDay:   day,
		updateTime:   ok3,
		lastPrice:    103,
		volume:       13,
		openInterest: 203,
		settlement:   99.3,
	})

	if err := spi.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	query := fmt.Sprintf(`SELECT COUNT(1) FROM "future_kline_instrument_1m_rb"`)
	row := store.DB().QueryRow(query)
	var cnt int
	if err := row.Scan(&cnt); err != nil {
		t.Fatalf("scan count failed: %v", err)
	}
	if cnt == 0 {
		t.Fatal("expected at least one row after drift resume")
	}
}

type tickInput struct {
	instrumentID string
	exchangeID   string
	actionDay    string
	tradingDay   string
	updateTime   string
	lastPrice    float64
	volume       int
	openInterest float64
	settlement   float64
}

func pushTick(t *testing.T, spi *testkit.MdSpi, in tickInput) {
	t.Helper()

	tick := ctp.NewCThostFtdcDepthMarketDataField()
	defer ctp.DeleteCThostFtdcDepthMarketDataField(tick)

	tick.SetInstrumentID(in.instrumentID)
	tick.SetExchangeID(in.exchangeID)
	tick.SetActionDay(in.actionDay)
	tick.SetTradingDay(in.tradingDay)
	tick.SetUpdateTime(in.updateTime)
	tick.SetLastPrice(in.lastPrice)
	tick.SetVolume(in.volume)
	tick.SetOpenInterest(in.openInterest)
	tick.SetSettlementPrice(in.settlement)

	spi.OnRtnDepthMarketData(tick)
}
