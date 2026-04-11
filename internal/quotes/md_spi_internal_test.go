package quotes

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestShouldCheckTickDriftNearTradingDayOnly(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.Local)
	sameDay := time.Date(2026, 2, 15, 9, 59, 0, 0, time.Local)
	prevDay := time.Date(2026, 2, 14, 23, 59, 59, 0, time.Local)
	oldDay := time.Date(2026, 2, 10, 9, 0, 0, 0, time.Local)

	if !shouldCheckTickDrift(now, sameDay) {
		t.Fatal("same day tick should check drift")
	}
	if !shouldCheckTickDrift(now, prevDay) {
		t.Fatal("prev day tick should check drift")
	}
	if shouldCheckTickDrift(now, oldDay) {
		t.Fatal("old day tick should not check drift")
	}
}

func TestSanitizeSettlementPriceResetsExtremeValueToZero(t *testing.T) {
	t.Parallel()

	if got := sanitizeSettlementPrice(math.MaxFloat64); got != 0 {
		t.Fatalf("sanitizeSettlementPrice(MaxFloat64) = %v, want 0", got)
	}
	if got := sanitizeSettlementPrice(5427.25); got != 5427.25 {
		t.Fatalf("sanitizeSettlementPrice(normal) = %v, want 5427.25", got)
	}
}

func TestSanitizeMarketDataFloatResetsNonFiniteValuesToZero(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   float64
		want float64
	}{
		{name: "nan", in: math.NaN(), want: 0},
		{name: "pos inf", in: math.Inf(1), want: 0},
		{name: "neg inf", in: math.Inf(-1), want: 0},
		{name: "huge", in: math.MaxFloat64, want: 0},
		{name: "normal", in: 12.345, want: 12.345},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := sanitizeMarketDataFloat(tc.in)
			if tc.want == 0 {
				if got != 0 {
					t.Fatalf("sanitizeMarketDataFloat(%v) = %v, want 0", tc.in, got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("sanitizeMarketDataFloat(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestFormatTickCSVLineKeepsThreeDecimals(t *testing.T) {
	t.Parallel()

	line := formatTickCSVLine(tickEvent{
		InstrumentID:       "rb2501",
		ExchangeID:         "SHFE",
		ExchangeInstID:     "rb2501",
		TradingDay:         "20260411",
		UpdateTime:         "09:30:01",
		ReceivedAt:         time.Date(2026, 4, 11, 9, 30, 1, 0, time.Local),
		LastPrice:          123.45678,
		PreSettlementPrice: 223.45678,
		PreClosePrice:      323.45678,
		PreOpenInterest:    423.45678,
		OpenPrice:          523.45678,
		HighestPrice:       623.45678,
		LowestPrice:        723.45678,
		Volume:             10,
		Turnover:           823.45678,
		OpenInterest:       923.45678,
		ClosePrice:         1023.45678,
		SettlementPrice:    1123.45678,
		UpperLimitPrice:    1223.45678,
		LowerLimitPrice:    1323.45678,
		AveragePrice:       1423.45678,
		PreDelta:           1523.45678,
		CurrDelta:          1623.45678,
		BidPrice1:          1723.45678,
		AskPrice1:          1823.45678,
		UpdateMillisec:     500,
		BidVolume1:         1,
		AskVolume1:         2,
	}, "20260410")

	if !strings.Contains(line, ",123.457,223.457,323.457,423.457,523.457,623.457,723.457,10,823.457,923.457,1023.457,1123.457,1223.457,1323.457,1423.457,1523.457,1623.457,1723.457,1823.457,500,1,2\n") {
		t.Fatalf("formatTickCSVLine() = %q, want 3-decimal csv line", line)
	}
}

func TestComputeBucketVolume(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		currentVol int
		prevVol    int
		hasPrev    bool
		want       int64
	}{
		{name: "first generated minute uses current volume", currentVol: 12, prevVol: 0, hasPrev: false, want: 12},
		{name: "normal delta", currentVol: 28, prevVol: 20, hasPrev: true, want: 8},
		{name: "counter reset falls back to current", currentVol: 6, prevVol: 20, hasPrev: true, want: 6},
		{name: "negative current clamps to zero", currentVol: -1, prevVol: 20, hasPrev: true, want: 0},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := computeBucketVolume(tc.currentVol, tc.prevVol, tc.hasPrev); got != tc.want {
				t.Fatalf("computeBucketVolume(%d,%d,%v)=%d want %d", tc.currentVol, tc.prevVol, tc.hasPrev, got, tc.want)
			}
		})
	}
}
