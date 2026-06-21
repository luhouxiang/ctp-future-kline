package strategy

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestMA20Indicators(t *testing.T) {
	bars := makeBarsFromCloses(1, 130, func(i int) float64 { return float64(i) })
	cfg := DefaultMA20BacktestConfig()
	ind := calcMA20Indicators(bars, cfg)
	if got := ind.MA20[129]; got != 119.5 {
		t.Fatalf("MA20 = %v, want 119.5", got)
	}
	if got := ind.MA60[129]; got != 99.5 {
		t.Fatalf("MA60 = %v, want 99.5", got)
	}
	if got := ind.MA120[129]; got != 69.5 {
		t.Fatalf("MA120 = %v, want 69.5", got)
	}
	if ind.MA20Slope[129] <= 0 || ind.MA60Slope[129] <= 0 || ind.MA120Slope[129] <= 0 {
		t.Fatalf("slopes should be positive: %+v %+v %+v", ind.MA20Slope[129], ind.MA60Slope[129], ind.MA120Slope[129])
	}
	if got := ind.LastSwingLow[129]; got <= 0 || got >= bars[129].Low {
		t.Fatalf("last swing low = %v, want prior low below current low", got)
	}
}

func TestMA20HardFilterRejectsStrongUptrendPullback(t *testing.T) {
	bars := makeBarsFromCloses(100, 130, func(i int) float64 { return 100 + float64(i)*0.2 })
	last := bars[len(bars)-1].Close
	bars = append(bars, testBar(len(bars), last-3.2))
	cfg := DefaultMA20BacktestConfig()
	out := RunMA20BacktestOnBars("future_kline_l9_mm_y", "yl9", bars, cfg, []string{MA20AlgoHardFilter})
	attempts := out[MA20AlgoHardFilter]
	if len(attempts) != 1 {
		t.Fatalf("attempts len=%d, want 1", len(attempts))
	}
	if attempts[0].Outcome != MA20OutcomeFiltered {
		t.Fatalf("outcome=%s, want filtered", attempts[0].Outcome)
	}
	if !strings.Contains(attempts[0].Reason, "strong uptrend") {
		t.Fatalf("reason=%q, want strong uptrend filter", attempts[0].Reason)
	}
}

func TestMA20WeakPullbackProducesSuccessfulSignal(t *testing.T) {
	bars := makeBarsFromCloses(100, 120, func(i int) float64 { return 100 })
	bars = append(bars,
		barWithOHLC(120, 100, 100.2, 98.7, 98.9),
		barWithOHLC(121, 99.2, 100.1, 98.8, 99.0),
		barWithOHLC(122, 99.0, 99.2, 98.7, 98.9),
		barWithOHLC(123, 98.7, 99.1, 97.4, 97.8),
		barWithOHLC(124, 97.9, 98.1, 96.5, 96.8),
		barWithOHLC(125, 96.8, 97.0, 96.2, 96.5),
		barWithOHLC(126, 96.6, 100.2, 96.4, 98.0),
		barWithOHLC(127, 98.1, 100.4, 96.8, 98.4),
		barWithOHLC(128, 97.8, 98.0, 96.9, 97.5),
		barWithOHLC(129, 97.0, 97.2, 96.1, 96.6),
		barWithOHLC(130, 96.1, 96.4, 95.4, 95.8),
		barWithOHLC(131, 95.7, 96.0, 95.0, 95.3),
		barWithOHLC(132, 95.4, 96.2, 95.2, 95.9),
		barWithOHLC(133, 95.9, 96.8, 95.5, 96.4),
		barWithOHLC(134, 96.5, 97.4, 96.0, 97.1),
		barWithOHLC(135, 97.1, 98.0, 96.7, 97.7),
		barWithOHLC(136, 97.7, 98.6, 97.3, 98.3),
		barWithOHLC(137, 98.2, 98.8, 97.8, 98.6),
		barWithOHLC(138, 98.4, 98.8, 97.9, 98.6),
		barWithOHLC(139, 98.4, 98.8, 98.0, 98.6),
		barWithOHLC(140, 98.4, 98.8, 98.1, 98.6),
		barWithOHLC(141, 98.4, 98.8, 98.2, 98.6),
	)
	cfg := DefaultMA20BacktestConfig()
	out := RunMA20BacktestOnBars("future_kline_l9_mm_y", "yl9", bars, cfg, []string{MA20AlgoHardFilter, MA20AlgoScoreFilter})
	for _, algo := range []string{MA20AlgoHardFilter, MA20AlgoScoreFilter} {
		attempts := out[algo]
		if len(attempts) != 1 {
			t.Fatalf("%s attempts len=%d, want 1", algo, len(attempts))
		}
		if attempts[0].Outcome != MA20OutcomeSuccess {
			t.Fatalf("%s outcome=%s reason=%s, want success", algo, attempts[0].Outcome, attempts[0].Reason)
		}
		if attempts[0].SignalIndex != 122 {
			t.Fatalf("%s signal index=%d, want 122", algo, attempts[0].SignalIndex)
		}
	}
}

func TestMA20BacktestContinuesScanningAfterSignal(t *testing.T) {
	bars := makeBarsFromCloses(100, 120, func(i int) float64 { return 100 })
	bars = append(bars,
		barWithOHLC(120, 100.0, 100.2, 98.7, 98.9),
		barWithOHLC(121, 99.2, 100.1, 98.8, 99.0),
		barWithOHLC(122, 99.0, 99.2, 98.7, 98.9),
		barWithOHLC(123, 100.5, 101.0, 100.1, 100.8),
		barWithOHLC(124, 100.0, 100.2, 98.5, 98.8),
		barWithOHLC(125, 99.1, 100.0, 98.7, 98.9),
		barWithOHLC(126, 98.8, 99.0, 98.1, 98.4),
	)
	cfg := DefaultMA20BacktestConfig()
	cfg.ObservationBars = 20
	cfg.ProfitReboundATR = 100
	cfg.StrongBullATR = 100
	cfg.StrengthExitBars = 100
	out := RunMA20BacktestOnBars("future_kline_l9_mm_y", "yl9", bars, cfg, []string{MA20AlgoBaseline})
	attempts := out[MA20AlgoBaseline]
	signals := 0
	indexes := []int{}
	for _, attempt := range attempts {
		if attempt.SignalIndex > 0 {
			signals++
			indexes = append(indexes, attempt.SignalIndex)
		}
	}
	if signals != 2 {
		t.Fatalf("signals=%d indexes=%v attempts=%+v, want two signals while first observation window is active", signals, indexes, attempts)
	}
}

func TestMA20OutcomeSameBarProfitAndAdverseCountsFailure(t *testing.T) {
	bars := []MA20BacktestBar{
		barWithOHLC(0, 100, 100, 100, 100),
		barWithOHLC(1, 100, 101, 97, 100.5),
	}
	attempt := MA20AttemptRecord{
		EntryPrice:    99,
		ProfitTarget:  98,
		AdverseTarget: 100,
	}
	idx := evaluateSignalOutcome(&attempt, bars, 0, DefaultMA20BacktestConfig())
	if idx != 1 {
		t.Fatalf("outcome idx=%d, want 1", idx)
	}
	if attempt.Outcome != MA20OutcomeFailure {
		t.Fatalf("outcome=%s, want failure", attempt.Outcome)
	}
}

func TestMA20QueryRejectsInvalidTableName(t *testing.T) {
	_, err := queryMA20BacktestBars(context.Background(), (*sql.DB)(nil), "bad-table", DefaultMA20BacktestConfig())
	if err == nil || !strings.Contains(err.Error(), "invalid kline table name") {
		t.Fatalf("error=%v, want invalid table name", err)
	}
}

func TestMA20BacktestRequestRoutesConcreteInstrumentToInstrumentTable(t *testing.T) {
	req := BacktestRequest{Symbol: "ao2609"}
	tables := ma20BacktestTablesFromRequest(req)
	if len(tables) != 1 || tables[0] != "future_kline_instrument_mm_ao" {
		t.Fatalf("tables = %v, want concrete instrument table", tables)
	}
	instruments := ma20BacktestInstrumentsFromRequest(req)
	if len(instruments) != 1 || instruments[0] != "ao2609" {
		t.Fatalf("instruments = %v, want ao2609 filter", instruments)
	}
}

func makeBarsFromCloses(start float64, count int, closeAt func(int) float64) []MA20BacktestBar {
	out := make([]MA20BacktestBar, 0, count)
	for i := 0; i < count; i++ {
		close := closeAt(i)
		out = append(out, barWithOHLC(i, close, close+0.5, close-0.5, close))
	}
	return out
}

func testBar(i int, close float64) MA20BacktestBar {
	return barWithOHLC(i, close+0.2, close+0.4, close-0.6, close)
}

func barWithOHLC(i int, open float64, high float64, low float64, close float64) MA20BacktestBar {
	ts := time.Date(2026, 1, 1, 9, 0, 0, 0, time.Local).Add(time.Duration(i) * 5 * time.Minute)
	return MA20BacktestBar{
		Table:        "future_kline_l9_mm_y",
		InstrumentID: "yl9",
		DataTime:     ts,
		AdjustedTime: ts,
		Open:         open,
		High:         high,
		Low:          low,
		Close:        close,
		Volume:       1,
	}
}
