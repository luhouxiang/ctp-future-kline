package strategy

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	MA20WeakStrategyID             = "ma20.weak_pullback_short"
	MA20WeakBaselineStrategyID     = "ma20.weak_pullback_short.baseline"
	MA20WeakHardFilterStrategyID   = "ma20.weak_pullback_short.hard_filter"
	MA20WeakScoreFilterStrategyID  = "ma20.weak_pullback_short.score_filter"

	MA20AlgoBaseline    = "baseline"
	MA20AlgoHardFilter  = "hard_filter"
	MA20AlgoScoreFilter = "score_filter"

	MA20OutcomeSuccess    = "success"
	MA20OutcomeFailure    = "failure"
	MA20OutcomeFiltered   = "filtered"
	MA20OutcomeNoSignal   = "no_signal"
	MA20OutcomeUnresolved = "unresolved"
)

func IsMA20WeakStrategyID(strategyID string) bool {
	switch strings.ToLower(strings.TrimSpace(strategyID)) {
	case MA20WeakStrategyID, MA20WeakBaselineStrategyID, MA20WeakHardFilterStrategyID, MA20WeakScoreFilterStrategyID:
		return true
	default:
		return false
	}
}

func MA20AlgorithmForStrategyID(strategyID string) string {
	switch strings.ToLower(strings.TrimSpace(strategyID)) {
	case MA20WeakBaselineStrategyID:
		return MA20AlgoBaseline
	case MA20WeakHardFilterStrategyID:
		return MA20AlgoHardFilter
	case MA20WeakScoreFilterStrategyID:
		return MA20AlgoScoreFilter
	default:
		return ""
	}
}

var DefaultMA20BacktestTables = []string{
	"future_kline_l9_mm_sr",
	"future_kline_l9_mm_ss",
	"future_kline_l9_mm_sp",
	"future_kline_l9_mm_y",
	"future_kline_l9_mm_ao",
}

var DefaultMA20BacktestAlgorithms = []string{
	MA20AlgoBaseline,
	MA20AlgoHardFilter,
	MA20AlgoScoreFilter,
}

type MA20BacktestConfig struct {
	Tables               []string
	Algorithms           []string
	Period               string
	StartTime            time.Time
	EndTime              time.Time
	ObservationBars      int
	ProfitATRMultiple    float64
	AdverseATRMultiple   float64
	StructureWaitBars    int
	TouchWaitBars        int
	TriggerWaitBars      int
	SwingLookbackBars    int
	SlopeLookbackBars    int
	ReportAttemptLimit   int
	BacktestStartedAt    time.Time
	BacktestFinishedAt   time.Time
	IncludeAttemptDetail bool
}

type MA20BacktestBar struct {
	Table        string    `json:"table"`
	InstrumentID string    `json:"instrument_id"`
	DataTime     time.Time `json:"data_time"`
	AdjustedTime time.Time `json:"adjusted_time"`
	Open         float64   `json:"open"`
	High         float64   `json:"high"`
	Low          float64   `json:"low"`
	Close        float64   `json:"close"`
	Volume       int64     `json:"volume"`
	OpenInterest float64   `json:"open_interest"`
}

type MA20AttemptRecord struct {
	ID                string             `json:"id"`
	Algorithm         string             `json:"algorithm"`
	Table             string             `json:"table"`
	InstrumentID      string             `json:"instrument_id"`
	StartIndex        int                `json:"start_index"`
	StartTime         time.Time          `json:"start_time"`
	SignalIndex       int                `json:"signal_index,omitempty"`
	SignalTime        *time.Time         `json:"signal_time,omitempty"`
	OutcomeIndex      int                `json:"outcome_index,omitempty"`
	OutcomeTime       *time.Time         `json:"outcome_time,omitempty"`
	Outcome           string             `json:"outcome"`
	Reason            string             `json:"reason"`
	Regime            string             `json:"regime,omitempty"`
	EntryPrice        float64            `json:"entry_price,omitempty"`
	ReactionLow       float64            `json:"reaction_low,omitempty"`
	TouchOpen         float64            `json:"touch_open,omitempty"`
	TouchHigh         float64            `json:"touch_high,omitempty"`
	ATR               float64            `json:"atr,omitempty"`
	ProfitTarget      float64            `json:"profit_target,omitempty"`
	AdverseTarget     float64            `json:"adverse_target,omitempty"`
	LastSwingLow      float64            `json:"last_swing_low,omitempty"`
	BullishPauseScore float64            `json:"bullish_pause_score,omitempty"`
	BearishScore      float64            `json:"bearish_failure_score,omitempty"`
	Steps             []MA20AttemptStep  `json:"steps,omitempty"`
	Metrics           map[string]float64 `json:"metrics,omitempty"`
}

type MA20AttemptStep struct {
	Index     int                `json:"index"`
	Time      time.Time          `json:"time"`
	StepKey   string             `json:"step_key"`
	StepLabel string             `json:"step_label"`
	Status    string             `json:"status"`
	Reason    string             `json:"reason"`
	Price     float64            `json:"price,omitempty"`
	Metrics   map[string]float64 `json:"metrics,omitempty"`
}

type MA20BacktestStats struct {
	AttemptsStarted     int     `json:"attempts_started"`
	Filtered            int     `json:"filtered"`
	FailedSetups        int     `json:"failed_setups"`
	Signals             int     `json:"signals"`
	Success             int     `json:"success"`
	Failure             int     `json:"failure"`
	Unresolved          int     `json:"unresolved"`
	AttemptSuccessRate  float64 `json:"attempt_success_rate"`
	SignalSuccessRate   float64 `json:"signal_success_rate"`
	SignalFormationRate float64 `json:"signal_formation_rate"`
}

type MA20BacktestResult struct {
	Config     map[string]any                          `json:"config"`
	Stats      map[string]MA20BacktestStats            `json:"stats"`
	TableStats map[string]map[string]MA20BacktestStats `json:"table_stats"`
	Attempts   []MA20AttemptRecord                     `json:"attempts"`
}

type ma20Indicators struct {
	MA20         []float64
	MA60         []float64
	MA120        []float64
	ATR14        []float64
	MA20Slope    []float64
	MA60Slope    []float64
	MA120Slope   []float64
	LastSwingLow []float64
}

func DefaultMA20BacktestConfig() MA20BacktestConfig {
	return MA20BacktestConfig{
		Tables:               append([]string(nil), DefaultMA20BacktestTables...),
		Algorithms:           append([]string(nil), DefaultMA20BacktestAlgorithms...),
		Period:               "5m",
		ObservationBars:      24,
		ProfitATRMultiple:    1.0,
		AdverseATRMultiple:   0.8,
		StructureWaitBars:    3,
		TouchWaitBars:        12,
		TriggerWaitBars:      6,
		SwingLookbackBars:    20,
		SlopeLookbackBars:    5,
		ReportAttemptLimit:   2000,
		IncludeAttemptDetail: true,
	}
}

func NormalizeMA20BacktestConfig(cfg MA20BacktestConfig) MA20BacktestConfig {
	def := DefaultMA20BacktestConfig()
	if len(cfg.Tables) == 0 {
		cfg.Tables = def.Tables
	}
	if len(cfg.Algorithms) == 0 {
		cfg.Algorithms = def.Algorithms
	}
	if strings.TrimSpace(cfg.Period) == "" {
		cfg.Period = def.Period
	}
	if cfg.ObservationBars <= 0 {
		cfg.ObservationBars = def.ObservationBars
	}
	if cfg.ProfitATRMultiple <= 0 {
		cfg.ProfitATRMultiple = def.ProfitATRMultiple
	}
	if cfg.AdverseATRMultiple <= 0 {
		cfg.AdverseATRMultiple = def.AdverseATRMultiple
	}
	if cfg.StructureWaitBars <= 0 {
		cfg.StructureWaitBars = def.StructureWaitBars
	}
	if cfg.TouchWaitBars <= 0 {
		cfg.TouchWaitBars = def.TouchWaitBars
	}
	if cfg.TriggerWaitBars <= 0 {
		cfg.TriggerWaitBars = def.TriggerWaitBars
	}
	if cfg.SwingLookbackBars <= 0 {
		cfg.SwingLookbackBars = def.SwingLookbackBars
	}
	if cfg.SlopeLookbackBars <= 0 {
		cfg.SlopeLookbackBars = def.SlopeLookbackBars
	}
	if cfg.ReportAttemptLimit <= 0 {
		cfg.ReportAttemptLimit = def.ReportAttemptLimit
	}
	cfg.Period = strings.ToLower(strings.TrimSpace(cfg.Period))
	cfg.Tables = sanitizeMA20List(cfg.Tables)
	cfg.Algorithms = sanitizeMA20List(cfg.Algorithms)
	return cfg
}

func RunMA20Backtest(ctx context.Context, db *sql.DB, cfg MA20BacktestConfig) (BacktestResponse, error) {
	cfg = NormalizeMA20BacktestConfig(cfg)
	started := time.Now()
	result := MA20BacktestResult{
		Config: map[string]any{
			"period":                 cfg.Period,
			"tables":                 cfg.Tables,
			"algorithms":             cfg.Algorithms,
			"observation_bars":       cfg.ObservationBars,
			"profit_atr_multiple":    cfg.ProfitATRMultiple,
			"adverse_atr_multiple":   cfg.AdverseATRMultiple,
			"structure_wait_bars":    cfg.StructureWaitBars,
			"touch_wait_bars":        cfg.TouchWaitBars,
			"trigger_wait_bars":      cfg.TriggerWaitBars,
			"swing_lookback_bars":    cfg.SwingLookbackBars,
			"slope_lookback_bars":    cfg.SlopeLookbackBars,
			"include_attempt_detail": cfg.IncludeAttemptDetail,
		},
		Stats:      map[string]MA20BacktestStats{},
		TableStats: map[string]map[string]MA20BacktestStats{},
	}
	if !cfg.StartTime.IsZero() {
		result.Config["start_time"] = cfg.StartTime.Format(time.RFC3339)
	}
	if !cfg.EndTime.IsZero() {
		result.Config["end_time"] = cfg.EndTime.Format(time.RFC3339)
	}
	for _, table := range cfg.Tables {
		bars, err := queryMA20BacktestBars(ctx, db, table, cfg)
		if err != nil {
			return BacktestResponse{}, err
		}
		grouped := groupMA20BarsByInstrument(bars)
		result.TableStats[table] = map[string]MA20BacktestStats{}
		for _, algo := range cfg.Algorithms {
			tableStats := MA20BacktestStats{}
			for _, instrument := range sortedInstrumentKeys(grouped) {
				instBars := grouped[instrument]
				if len(instBars) < 130 {
					continue
				}
				ind := calcMA20Indicators(instBars, cfg)
				attempts := runMA20Algorithm(table, instrument, instBars, ind, algo, cfg)
				for _, attempt := range attempts {
					applyMA20Stats(&tableStats, attempt)
					if cfg.IncludeAttemptDetail && len(result.Attempts) < cfg.ReportAttemptLimit {
						result.Attempts = append(result.Attempts, attempt)
					}
				}
			}
			finalizeMA20Stats(&tableStats)
			result.TableStats[table][algo] = tableStats
			total := result.Stats[algo]
			addMA20Stats(&total, tableStats)
			result.Stats[algo] = total
		}
	}
	for algo, stats := range result.Stats {
		finalizeMA20Stats(&stats)
		result.Stats[algo] = stats
	}
	result.Config["generated_at"] = time.Now().Format(time.RFC3339Nano)
	result.Config["elapsed_ms"] = time.Since(started).Milliseconds()
	return BacktestResponse{
		Status: "done",
		Summary: map[string]any{
			"strategy_id": MA20WeakStrategyID,
			"period":      cfg.Period,
			"tables":      cfg.Tables,
			"algorithms":  cfg.Algorithms,
			"stats":       result.Stats,
			"table_stats": result.TableStats,
		},
		Result: map[string]any{
			"config":      result.Config,
			"stats":       result.Stats,
			"table_stats": result.TableStats,
			"attempts":    result.Attempts,
		},
	}, nil
}

func queryMA20BacktestBars(ctx context.Context, db *sql.DB, table string, cfg MA20BacktestConfig) ([]MA20BacktestBar, error) {
	if !safeSQLIdent(table) {
		return nil, fmt.Errorf("invalid kline table name: %s", table)
	}
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(1) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?`, table).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, fmt.Errorf("kline table does not exist: %s", table)
	}
	where := []string{`"Period"=?`}
	args := []any{cfg.Period}
	if !cfg.StartTime.IsZero() {
		where = append(where, `"AdjustedTime">=?`)
		args = append(args, cfg.StartTime.Format("2006-01-02 15:04:05"))
	}
	if !cfg.EndTime.IsZero() {
		where = append(where, `"AdjustedTime"<=?`)
		args = append(args, cfg.EndTime.Format("2006-01-02 15:04:05"))
	}
	query := fmt.Sprintf(`
SELECT "InstrumentID","DataTime","AdjustedTime","Open","High","Low","Close","Volume","OpenInterest"
FROM "%s"
WHERE %s
ORDER BY "InstrumentID" ASC, "AdjustedTime" ASC`, table, strings.Join(where, " AND "))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query %s failed: %w", table, err)
	}
	defer rows.Close()
	out := make([]MA20BacktestBar, 0, 8192)
	for rows.Next() {
		var b MA20BacktestBar
		b.Table = table
		if err := rows.Scan(&b.InstrumentID, &b.DataTime, &b.AdjustedTime, &b.Open, &b.High, &b.Low, &b.Close, &b.Volume, &b.OpenInterest); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func RunMA20BacktestOnBars(table string, instrument string, bars []MA20BacktestBar, cfg MA20BacktestConfig, algorithms []string) map[string][]MA20AttemptRecord {
	cfg = NormalizeMA20BacktestConfig(cfg)
	if len(algorithms) == 0 {
		algorithms = cfg.Algorithms
	}
	ind := calcMA20Indicators(bars, cfg)
	out := make(map[string][]MA20AttemptRecord, len(algorithms))
	for _, algo := range algorithms {
		out[algo] = runMA20Algorithm(table, instrument, bars, ind, algo, cfg)
	}
	return out
}

func calcMA20Indicators(bars []MA20BacktestBar, cfg MA20BacktestConfig) ma20Indicators {
	n := len(bars)
	out := ma20Indicators{
		MA20:         make([]float64, n),
		MA60:         make([]float64, n),
		MA120:        make([]float64, n),
		ATR14:        make([]float64, n),
		MA20Slope:    make([]float64, n),
		MA60Slope:    make([]float64, n),
		MA120Slope:   make([]float64, n),
		LastSwingLow: make([]float64, n),
	}
	closes := make([]float64, n)
	tr := make([]float64, n)
	for i, b := range bars {
		closes[i] = b.Close
		prevClose := b.Close
		if i > 0 {
			prevClose = bars[i-1].Close
		}
		tr[i] = math.Max(b.High-b.Low, math.Max(math.Abs(b.High-prevClose), math.Abs(b.Low-prevClose)))
		out.MA20[i] = rollingAvg(closes, i, 20)
		out.MA60[i] = rollingAvg(closes, i, 60)
		out.MA120[i] = rollingAvg(closes, i, 120)
		out.ATR14[i] = rollingAvg(tr, i, 14)
		if i >= cfg.SlopeLookbackBars {
			out.MA20Slope[i] = slopeValue(out.MA20, i, cfg.SlopeLookbackBars)
			out.MA60Slope[i] = slopeValue(out.MA60, i, cfg.SlopeLookbackBars)
			out.MA120Slope[i] = slopeValue(out.MA120, i, cfg.SlopeLookbackBars)
		}
		out.LastSwingLow[i] = priorLowestLow(bars, i, cfg.SwingLookbackBars)
	}
	return out
}

func runMA20Algorithm(table string, instrument string, bars []MA20BacktestBar, ind ma20Indicators, algo string, cfg MA20BacktestConfig) []MA20AttemptRecord {
	algo = strings.ToLower(strings.TrimSpace(algo))
	if algo == "" {
		algo = MA20AlgoHardFilter
	}
	attempts := []MA20AttemptRecord{}
	for i := 1; i < len(bars); i++ {
		if !readyMA20Index(ind, i) {
			continue
		}
		if !crossBreakMA20(bars, ind, i) {
			continue
		}
		attempt := newMA20Attempt(table, instrument, algo, len(attempts)+1, i, bars[i], ind)
		switch algo {
		case MA20AlgoBaseline:
			i = runBaselineAttempt(&attempt, bars, ind, i, cfg)
		case MA20AlgoHardFilter, MA20AlgoScoreFilter:
			i = runFilteredAttempt(&attempt, bars, ind, i, cfg, algo == MA20AlgoScoreFilter)
		default:
			attempt.Outcome = MA20OutcomeFiltered
			attempt.Reason = "unknown algorithm"
		}
		attempts = append(attempts, attempt)
	}
	return attempts
}

func runBaselineAttempt(attempt *MA20AttemptRecord, bars []MA20BacktestBar, ind ma20Indicators, start int, cfg MA20BacktestConfig) int {
	addAttemptStep(attempt, bars[start], start, "WAIT_BREAK_BELOW_MA20", "跌破 MA20", "passed", "break below MA20 confirmed", bars[start].Close, indicatorMetrics(ind, start))
	touchIndex := -1
	last := minInt(len(bars)-1, start+cfg.TouchWaitBars)
	for j := start + 1; j <= last; j++ {
		if stoodAboveMA20(bars, ind, j) {
			attempt.Outcome = MA20OutcomeNoSignal
			attempt.Reason = "reset: stood above MA20 before touch"
			addAttemptStep(attempt, bars[j], j, "WAIT_PULLBACK_TOUCH_MA20", "等待反抽触碰 MA20", "failed", attempt.Reason, bars[j].Close, indicatorMetrics(ind, j))
			return j
		}
		if bars[j].High >= ind.MA20[j] && ind.MA20[j] > 0 {
			touchIndex = j
			attempt.TouchOpen = bars[j].Open
			attempt.TouchHigh = bars[j].High
			attempt.EntryPrice = bars[j].Open
			addAttemptStep(attempt, bars[j], j, "WAIT_PULLBACK_TOUCH_MA20", "反抽触碰 MA20", "passed", "MA20 touch bar found", bars[j].High, indicatorMetrics(ind, j))
			break
		}
	}
	if touchIndex < 0 {
		attempt.Outcome = MA20OutcomeNoSignal
		attempt.Reason = "touch wait timeout"
		return last
	}
	triggerLast := minInt(len(bars)-1, touchIndex+cfg.TriggerWaitBars)
	for j := touchIndex + 1; j <= triggerLast; j++ {
		if stoodAboveMA20(bars, ind, j) {
			attempt.Outcome = MA20OutcomeNoSignal
			attempt.Reason = "reset: stood above MA20 before entry"
			addAttemptStep(attempt, bars[j], j, "WAIT_BREAK_TOUCH_OPEN", "等待跌破触碰 K 开盘价", "failed", attempt.Reason, bars[j].Close, indicatorMetrics(ind, j))
			return j
		}
		if bars[j].Low <= attempt.TouchOpen {
			addSignal(attempt, bars, ind, j, attempt.TouchOpen, cfg, "SHORT: broke touch bar open")
			return evaluateSignalOutcome(attempt, bars, j, cfg)
		}
	}
	attempt.Outcome = MA20OutcomeNoSignal
	attempt.Reason = "entry wait timeout"
	return triggerLast
}

func runFilteredAttempt(attempt *MA20AttemptRecord, bars []MA20BacktestBar, ind ma20Indicators, start int, cfg MA20BacktestConfig, useScore bool) int {
	addAttemptStep(attempt, bars[start], start, "WAIT_BREAK_BELOW_MA20", "跌破 MA20，进入趋势过滤", "passed", "break below MA20 confirmed", bars[start].Close, indicatorMetrics(ind, start))
	structureIndex := -1
	lastStructure := minInt(len(bars)-1, start+cfg.StructureWaitBars)
	for j := start; j <= lastStructure; j++ {
		strongUp := strongUptrend(bars, ind, j)
		structureBroken := structureBroken(bars, ind, j)
		bull, bear := scoreMA20Setup(bars, ind, start, j, 0, 0)
		attempt.BullishPauseScore = bull
		attempt.BearishScore = bear
		if strongUp && !structureBroken {
			attempt.Outcome = MA20OutcomeFiltered
			attempt.Reason = "strong uptrend pullback without structure break"
			attempt.Regime = "BULLISH_PULLBACK"
			addAttemptStep(attempt, bars[j], j, "TREND_STRUCTURE_FILTER", "趋势/结构过滤", "failed", attempt.Reason, bars[j].Close, scoreMetrics(ind, j, bull, bear))
			return j
		}
		if j > start && bars[j].Close > ind.MA20[j] && !structureBroken {
			attempt.Outcome = MA20OutcomeFiltered
			attempt.Reason = "BULLISH_PAUSE: fast reclaim above MA20"
			attempt.Regime = "BULLISH_PAUSE"
			addAttemptStep(attempt, bars[j], j, "TREND_STRUCTURE_FILTER", "快速收回 MA20", "failed", attempt.Reason, bars[j].Close, scoreMetrics(ind, j, bull, bear))
			return j
		}
		if structureBroken {
			structureIndex = j
			break
		}
	}
	if structureIndex < 0 {
		attempt.Outcome = MA20OutcomeFiltered
		attempt.Reason = "structure break not confirmed"
		attempt.Regime = "UNCLEAR"
		return lastStructure
	}
	regime := classifyBearishRegime(bars, ind, structureIndex)
	attempt.Regime = regime
	if regime != "BEARISH_REVERSAL_CANDIDATE" && regime != "WEAK_BEARISH_CANDIDATE" {
		attempt.Outcome = MA20OutcomeFiltered
		attempt.Reason = "not bearish regime"
		addAttemptStep(attempt, bars[structureIndex], structureIndex, "TREND_STRUCTURE_FILTER", "趋势/结构过滤", "failed", attempt.Reason, bars[structureIndex].Close, indicatorMetrics(ind, structureIndex))
		return structureIndex
	}
	addAttemptStep(attempt, bars[structureIndex], structureIndex, "TREND_STRUCTURE_FILTER", "趋势/结构过滤通过", "passed", regime, bars[structureIndex].Close, indicatorMetrics(ind, structureIndex))

	touchIndex := -1
	lastTouch := minInt(len(bars)-1, structureIndex+cfg.TouchWaitBars)
	for j := structureIndex + 1; j <= lastTouch; j++ {
		if stoodAboveMA20(bars, ind, j) {
			attempt.Outcome = MA20OutcomeNoSignal
			attempt.Reason = "reset: stood above MA20 before weak pullback"
			addAttemptStep(attempt, bars[j], j, "WAIT_PULLBACK_TOUCH_MA20", "等待弱反触碰 MA20", "failed", attempt.Reason, bars[j].Close, indicatorMetrics(ind, j))
			return j
		}
		if bars[j].High >= ind.MA20[j] && ind.MA20[j] > 0 {
			touchIndex = j
			attempt.TouchOpen = bars[j].Open
			attempt.TouchHigh = bars[j].High
			attempt.ReactionLow = lowestLowInRange(bars, start, j)
			addAttemptStep(attempt, bars[j], j, "WAIT_PULLBACK_TOUCH_MA20", "弱反触碰 MA20", "passed", "weak pullback touched MA20", bars[j].High, indicatorMetrics(ind, j))
			break
		}
	}
	if touchIndex < 0 {
		attempt.Outcome = MA20OutcomeNoSignal
		attempt.Reason = "weak pullback touch timeout"
		return lastTouch
	}
	triggerLast := minInt(len(bars)-1, touchIndex+cfg.TriggerWaitBars)
	for j := touchIndex + 1; j <= triggerLast; j++ {
		if stoodAboveMA20(bars, ind, j) {
			attempt.Outcome = MA20OutcomeNoSignal
			attempt.Reason = "reset: stood above MA20 before reaction low break"
			addAttemptStep(attempt, bars[j], j, "WAIT_BREAK_REACTION_LOW", "等待跌破反抽低点", "failed", attempt.Reason, bars[j].Close, indicatorMetrics(ind, j))
			return j
		}
		if bars[j].Low <= attempt.ReactionLow {
			bull, bear := scoreMA20Setup(bars, ind, start, j, touchIndex, attempt.ReactionLow)
			attempt.BullishPauseScore = bull
			attempt.BearishScore = bear
			if useScore && bull > bear {
				attempt.Outcome = MA20OutcomeFiltered
				attempt.Reason = "bullish pause score exceeds bearish failure score"
				addAttemptStep(attempt, bars[j], j, "WAIT_BREAK_REACTION_LOW", "评分过滤", "failed", attempt.Reason, bars[j].Close, scoreMetrics(ind, j, bull, bear))
				return j
			}
			addSignal(attempt, bars, ind, j, attempt.ReactionLow, cfg, "SHORT: broke weak pullback reaction low")
			return evaluateSignalOutcome(attempt, bars, j, cfg)
		}
	}
	attempt.Outcome = MA20OutcomeNoSignal
	attempt.Reason = "reaction low break timeout"
	return triggerLast
}

func addSignal(attempt *MA20AttemptRecord, bars []MA20BacktestBar, ind ma20Indicators, signalIndex int, entry float64, cfg MA20BacktestConfig, reason string) {
	ts := bars[signalIndex].AdjustedTime
	attempt.SignalIndex = signalIndex
	attempt.SignalTime = &ts
	attempt.EntryPrice = entry
	attempt.ATR = ind.ATR14[signalIndex]
	if attempt.ATR <= 0 {
		attempt.ATR = math.Max(0.0001, bars[signalIndex].High-bars[signalIndex].Low)
	}
	attempt.ProfitTarget = entry - cfg.ProfitATRMultiple*attempt.ATR
	attempt.AdverseTarget = entry + cfg.AdverseATRMultiple*attempt.ATR
	attempt.Reason = reason
	addAttemptStep(attempt, bars[signalIndex], signalIndex, "SHORT_SIGNAL", "做空信号", "passed", reason, entry, map[string]float64{
		"entry_price":    entry,
		"atr14":          attempt.ATR,
		"profit_target":  attempt.ProfitTarget,
		"adverse_target": attempt.AdverseTarget,
	})
}

func evaluateSignalOutcome(attempt *MA20AttemptRecord, bars []MA20BacktestBar, signalIndex int, cfg MA20BacktestConfig) int {
	last := minInt(len(bars)-1, signalIndex+cfg.ObservationBars)
	for j := signalIndex + 1; j <= last; j++ {
		hitAdverse := bars[j].High >= attempt.AdverseTarget
		hitProfit := bars[j].Low <= attempt.ProfitTarget
		if hitAdverse {
			ts := bars[j].AdjustedTime
			attempt.Outcome = MA20OutcomeFailure
			attempt.OutcomeIndex = j
			attempt.OutcomeTime = &ts
			attempt.Reason = "adverse target hit before profit target"
			addAttemptStep(attempt, bars[j], j, "SIGNAL_RESULT", "信号失败", "failed", attempt.Reason, attempt.AdverseTarget, nil)
			return j
		}
		if hitProfit {
			ts := bars[j].AdjustedTime
			attempt.Outcome = MA20OutcomeSuccess
			attempt.OutcomeIndex = j
			attempt.OutcomeTime = &ts
			attempt.Reason = "profit target hit before adverse target"
			addAttemptStep(attempt, bars[j], j, "SIGNAL_RESULT", "信号成功", "passed", attempt.Reason, attempt.ProfitTarget, nil)
			return j
		}
	}
	attempt.Outcome = MA20OutcomeUnresolved
	attempt.Reason = "observation window ended without profit/adverse target"
	return last
}

func newMA20Attempt(table string, instrument string, algo string, seq int, index int, bar MA20BacktestBar, ind ma20Indicators) MA20AttemptRecord {
	return MA20AttemptRecord{
		ID:           fmt.Sprintf("%s:%s:%s:%06d", table, instrument, algo, seq),
		Algorithm:    algo,
		Table:        table,
		InstrumentID: instrument,
		StartIndex:   index,
		StartTime:    bar.AdjustedTime,
		Outcome:      MA20OutcomeNoSignal,
		LastSwingLow: ind.LastSwingLow[index],
		Metrics:      indicatorMetrics(ind, index),
	}
}

func addAttemptStep(attempt *MA20AttemptRecord, bar MA20BacktestBar, index int, key string, label string, status string, reason string, price float64, metrics map[string]float64) {
	attempt.Steps = append(attempt.Steps, MA20AttemptStep{
		Index:     index,
		Time:      bar.AdjustedTime,
		StepKey:   key,
		StepLabel: label,
		Status:    status,
		Reason:    reason,
		Price:     price,
		Metrics:   metrics,
	})
}

func crossBreakMA20(bars []MA20BacktestBar, ind ma20Indicators, i int) bool {
	if i <= 0 || ind.MA20[i] <= 0 || ind.MA20[i-1] <= 0 {
		return false
	}
	return bars[i-1].Close >= ind.MA20[i-1] && bars[i].Close < ind.MA20[i]
}

func readyMA20Index(ind ma20Indicators, i int) bool {
	return i >= 120 && ind.MA20[i] > 0 && ind.MA60[i] > 0 && ind.MA120[i] > 0 && ind.ATR14[i] > 0
}

func stoodAboveMA20(bars []MA20BacktestBar, ind ma20Indicators, i int) bool {
	return ind.MA20[i] > 0 && bars[i].Open > ind.MA20[i] && bars[i].Close > ind.MA20[i]
}

func strongUptrend(bars []MA20BacktestBar, ind ma20Indicators, i int) bool {
	return ind.MA20[i] > ind.MA60[i] &&
		ind.MA60[i] > ind.MA120[i] &&
		ind.MA60Slope[i] > 0 &&
		ind.MA120Slope[i] > 0 &&
		bars[i].Close > ind.MA60[i]
}

func structureBroken(bars []MA20BacktestBar, ind ma20Indicators, i int) bool {
	return ind.LastSwingLow[i] > 0 && bars[i].Close < ind.LastSwingLow[i]
}

func classifyBearishRegime(bars []MA20BacktestBar, ind ma20Indicators, i int) string {
	if structureBroken(bars, ind, i) && ind.MA20Slope[i] < 0 {
		return "BEARISH_REVERSAL_CANDIDATE"
	}
	if bars[i].Close < ind.MA60[i] && ind.MA20Slope[i] < 0 && ind.MA60Slope[i] <= 0 {
		return "WEAK_BEARISH_CANDIDATE"
	}
	return "UNCLEAR"
}

func scoreMA20Setup(bars []MA20BacktestBar, ind ma20Indicators, start int, current int, touchIndex int, reactionLow float64) (float64, float64) {
	bull := 0.0
	bear := 0.0
	if strongUptrend(bars, ind, current) {
		bull += 2
	}
	if ind.MA20[current] > ind.MA60[current] && ind.MA60[current] > ind.MA120[current] {
		bull++
	}
	if !structureBroken(bars, ind, current) {
		bull += 2
	} else {
		bear += 2
	}
	breakStrength := ind.MA20[start] - bars[start].Close
	if ind.ATR14[start] > 0 && breakStrength < 0.5*ind.ATR14[start] {
		bull++
	}
	if ind.ATR14[start] > 0 && breakStrength >= 0.5*ind.ATR14[start] {
		bear++
	}
	if current-start <= 3 && bars[current].Close > ind.MA20[current] {
		bull++
	}
	if ind.MA20Slope[current] < 0 {
		bear++
	}
	if ind.MA60Slope[current] <= 0 {
		bear++
	}
	if bars[current].Close < ind.MA20[current] {
		bear++
	}
	if touchIndex > 0 && touchIndex-start <= 3 {
		bear++
	}
	if reactionLow > 0 && bars[current].Low <= reactionLow {
		bear += 2
	}
	if bars[current].Close > bars[current].Open {
		bull++
	} else if bars[current].Close < bars[current].Open {
		bear++
	}
	return bull, bear
}

func indicatorMetrics(ind ma20Indicators, i int) map[string]float64 {
	return map[string]float64{
		"ma20":           ind.MA20[i],
		"ma60":           ind.MA60[i],
		"ma120":          ind.MA120[i],
		"ma20_slope":     ind.MA20Slope[i],
		"ma60_slope":     ind.MA60Slope[i],
		"ma120_slope":    ind.MA120Slope[i],
		"atr14":          ind.ATR14[i],
		"last_swing_low": ind.LastSwingLow[i],
	}
}

func scoreMetrics(ind ma20Indicators, i int, bull float64, bear float64) map[string]float64 {
	m := indicatorMetrics(ind, i)
	m["bullish_pause_score"] = bull
	m["bearish_failure_score"] = bear
	return m
}

func rollingAvg(values []float64, i int, period int) float64 {
	if period <= 0 || i+1 < period {
		return 0
	}
	sum := 0.0
	for j := i - period + 1; j <= i; j++ {
		sum += values[j]
	}
	return sum / float64(period)
}

func slopeValue(values []float64, i int, lookback int) float64 {
	if lookback <= 0 || i < lookback || values[i] == 0 || values[i-lookback] == 0 {
		return 0
	}
	return (values[i] - values[i-lookback]) / float64(lookback)
}

func priorLowestLow(bars []MA20BacktestBar, i int, lookback int) float64 {
	start := i - lookback
	if start < 0 {
		start = 0
	}
	if start >= i {
		return 0
	}
	low := bars[start].Low
	for j := start + 1; j < i; j++ {
		if bars[j].Low < low {
			low = bars[j].Low
		}
	}
	return low
}

func lowestLowInRange(bars []MA20BacktestBar, start int, end int) float64 {
	if start < 0 {
		start = 0
	}
	if end >= len(bars) {
		end = len(bars) - 1
	}
	if start > end || len(bars) == 0 {
		return 0
	}
	low := bars[start].Low
	for i := start + 1; i <= end; i++ {
		if bars[i].Low < low {
			low = bars[i].Low
		}
	}
	return low
}

func applyMA20Stats(stats *MA20BacktestStats, attempt MA20AttemptRecord) {
	stats.AttemptsStarted++
	switch attempt.Outcome {
	case MA20OutcomeFiltered:
		stats.Filtered++
	case MA20OutcomeNoSignal:
		stats.FailedSetups++
	case MA20OutcomeSuccess:
		stats.Signals++
		stats.Success++
	case MA20OutcomeFailure:
		stats.Signals++
		stats.Failure++
	case MA20OutcomeUnresolved:
		stats.Signals++
		stats.Unresolved++
	}
}

func addMA20Stats(dst *MA20BacktestStats, src MA20BacktestStats) {
	dst.AttemptsStarted += src.AttemptsStarted
	dst.Filtered += src.Filtered
	dst.FailedSetups += src.FailedSetups
	dst.Signals += src.Signals
	dst.Success += src.Success
	dst.Failure += src.Failure
	dst.Unresolved += src.Unresolved
}

func finalizeMA20Stats(stats *MA20BacktestStats) {
	if stats.AttemptsStarted > 0 {
		stats.AttemptSuccessRate = float64(stats.Success) / float64(stats.AttemptsStarted)
		stats.SignalFormationRate = float64(stats.Signals) / float64(stats.AttemptsStarted)
	}
	if stats.Signals > 0 {
		stats.SignalSuccessRate = float64(stats.Success) / float64(stats.Signals)
	}
}

func groupMA20BarsByInstrument(bars []MA20BacktestBar) map[string][]MA20BacktestBar {
	out := map[string][]MA20BacktestBar{}
	for _, b := range bars {
		key := strings.ToLower(strings.TrimSpace(b.InstrumentID))
		if key == "" {
			key = "unknown"
		}
		out[key] = append(out[key], b)
	}
	return out
}

func sortedInstrumentKeys(items map[string][]MA20BacktestBar) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sanitizeMA20List(items []string) []string {
	out := make([]string, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		v := strings.ToLower(strings.TrimSpace(item))
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func safeSQLIdent(value string) bool {
	if value == "" {
		return false
	}
	for _, ch := range value {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			continue
		}
		return false
	}
	return true
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
