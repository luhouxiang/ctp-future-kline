package strategy

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type ReplaySignalReportRow struct {
	ID             int64          `json:"id"`
	EventTime      time.Time      `json:"event_time"`
	Symbol         string         `json:"symbol"`
	Timeframe      string         `json:"timeframe"`
	TargetPosition float64        `json:"target_position"`
	Confidence     float64        `json:"confidence"`
	Reason         string         `json:"reason"`
	Metrics        map[string]any `json:"metrics"`
}

type ReplayOrderReportRow struct {
	ID              int64          `json:"id"`
	EventTime       time.Time      `json:"event_time"`
	Symbol          string         `json:"symbol"`
	CurrentPosition float64        `json:"current_position"`
	TargetPosition  float64        `json:"target_position"`
	PlannedDelta    float64        `json:"planned_delta"`
	RiskStatus      string         `json:"risk_status"`
	RiskReason      string         `json:"risk_reason"`
	OrderStatus     string         `json:"order_status"`
	Audit           map[string]any `json:"audit"`
}

type ReplayAnalysisReportRow struct {
	ReplayTaskID      string     `json:"replay_task_id"`
	InstanceID        string     `json:"instance_id"`
	StrategyID        string     `json:"strategy_id"`
	Symbol            string     `json:"symbol"`
	Timeframe         string     `json:"timeframe"`
	SignalCount       int        `json:"signal_count"`
	OrderPlanCount    int        `json:"order_plan_count"`
	BlockedCount      int        `json:"blocked_count"`
	NoopCount         int        `json:"noop_count"`
	SimulatedCount    int        `json:"simulated_count"`
	FirstSignalAt     *time.Time `json:"first_signal_at,omitempty"`
	LastSignalAt      *time.Time `json:"last_signal_at,omitempty"`
	NetTargetChange   float64    `json:"net_target_position_change"`
	LatestOrderStatus string     `json:"latest_order_status"`
	LatestRiskStatus  string     `json:"latest_risk_status"`
	LatestRiskReason  string     `json:"latest_risk_reason"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

type ReplayReport struct {
	RunID        string                  `json:"run_id"`
	ReplayTaskID string                  `json:"replay_task_id"`
	InstanceID   string                  `json:"instance_id"`
	StrategyID   string                  `json:"strategy_id"`
	DisplayName  string                  `json:"display_name"`
	Symbol       string                  `json:"symbol"`
	Timeframe    string                  `json:"timeframe"`
	Status       string                  `json:"status"`
	StartedAt    time.Time               `json:"started_at"`
	FinishedAt   *time.Time              `json:"finished_at,omitempty"`
	SignalTable  []ReplaySignalReportRow `json:"signal_table"`
	OrderTable   []ReplayOrderReportRow  `json:"order_table"`
}

func (r *ReplayReport) result() map[string]any {
	signals := r.SignalTable
	if signals == nil {
		signals = []ReplaySignalReportRow{}
	}
	orders := r.OrderTable
	if orders == nil {
		orders = []ReplayOrderReportRow{}
	}
	return map[string]any{
		"replay_task_id":          r.ReplayTaskID,
		"instance_id":             r.InstanceID,
		"strategy_id":             r.StrategyID,
		"symbol":                  r.Symbol,
		"timeframe":               r.Timeframe,
		"status":                  r.Status,
		"signal_table":            signals,
		"order_table":             orders,
		"strategy_analysis_table": []ReplayAnalysisReportRow{r.analysis()},
	}
}

func (r *ReplayReport) summary() map[string]any {
	analysis := r.analysis()
	return map[string]any{
		"kind":                       RunTypeReplayReport,
		"replay_task_id":             r.ReplayTaskID,
		"instance_id":                r.InstanceID,
		"strategy_id":                r.StrategyID,
		"signal_count":               analysis.SignalCount,
		"order_plan_count":           analysis.OrderPlanCount,
		"blocked_count":              analysis.BlockedCount,
		"noop_count":                 analysis.NoopCount,
		"simulated_count":            analysis.SimulatedCount,
		"net_target_position_change": analysis.NetTargetChange,
		"latest_order_status":        analysis.LatestOrderStatus,
		"latest_risk_status":         analysis.LatestRiskStatus,
	}
}

func (r *ReplayReport) analysis() ReplayAnalysisReportRow {
	var firstSignal *time.Time
	var lastSignal *time.Time
	var firstTarget float64
	var lastTarget float64
	if len(r.SignalTable) > 0 {
		sort.SliceStable(r.SignalTable, func(i, j int) bool {
			return r.SignalTable[i].EventTime.Before(r.SignalTable[j].EventTime)
		})
		first := r.SignalTable[0].EventTime
		last := r.SignalTable[len(r.SignalTable)-1].EventTime
		firstSignal = &first
		lastSignal = &last
		firstTarget = r.SignalTable[0].TargetPosition
		lastTarget = r.SignalTable[len(r.SignalTable)-1].TargetPosition
	}
	analysis := ReplayAnalysisReportRow{
		ReplayTaskID:    r.ReplayTaskID,
		InstanceID:      r.InstanceID,
		StrategyID:      r.StrategyID,
		Symbol:          r.Symbol,
		Timeframe:       r.Timeframe,
		SignalCount:     len(r.SignalTable),
		OrderPlanCount:  len(r.OrderTable),
		FirstSignalAt:   firstSignal,
		LastSignalAt:    lastSignal,
		NetTargetChange: normalizeZero(lastTarget - firstTarget),
		UpdatedAt:       time.Now(),
	}
	if len(r.OrderTable) > 0 {
		sort.SliceStable(r.OrderTable, func(i, j int) bool {
			return r.OrderTable[i].EventTime.Before(r.OrderTable[j].EventTime)
		})
		for _, row := range r.OrderTable {
			switch strings.ToLower(strings.TrimSpace(row.OrderStatus)) {
			case OrderStatusBlocked:
				analysis.BlockedCount++
			case OrderStatusNoop:
				analysis.NoopCount++
			case OrderStatusSimulated:
				analysis.SimulatedCount++
			}
		}
		latest := r.OrderTable[len(r.OrderTable)-1]
		analysis.LatestOrderStatus = latest.OrderStatus
		analysis.LatestRiskStatus = latest.RiskStatus
		analysis.LatestRiskReason = latest.RiskReason
	}
	return analysis
}

func (m *Manager) appendReplayReport(replayTaskID string, inst StrategyInstance, sig SignalRecord, audit OrderAuditRecord) {
	if m == nil || m.store == nil || strings.ToLower(strings.TrimSpace(inst.Mode)) != RunTypeReplay {
		return
	}
	replayTaskID = replayReportScopeID(replayTaskID, inst)
	m.reportMu.Lock()
	defer m.reportMu.Unlock()
	report := m.replayReportLocked(replayTaskID, inst)
	report.SignalTable = append(report.SignalTable, ReplaySignalReportRow{
		ID:             sig.ID,
		EventTime:      sig.EventTime,
		Symbol:         sig.Symbol,
		Timeframe:      sig.Timeframe,
		TargetPosition: sig.TargetPosition,
		Confidence:     sig.Confidence,
		Reason:         sig.Reason,
		Metrics:        sig.Metrics,
	})
	report.OrderTable = append(report.OrderTable, ReplayOrderReportRow{
		ID:              audit.ID,
		EventTime:       audit.EventTime,
		Symbol:          audit.Symbol,
		CurrentPosition: audit.CurrentPosition,
		TargetPosition:  audit.TargetPosition,
		PlannedDelta:    audit.PlannedDelta,
		RiskStatus:      audit.RiskStatus,
		RiskReason:      audit.RiskReason,
		OrderStatus:     audit.OrderStatus,
		Audit:           audit.Audit,
	})
	if err := m.saveReplayReportLocked(report); err != nil {
		m.setError(err)
	}
}

func (m *Manager) touchReplayReport(replayTaskID string, inst StrategyInstance) {
	if m == nil || m.store == nil || strings.ToLower(strings.TrimSpace(inst.Mode)) != RunTypeReplay {
		return
	}
	replayTaskID = replayReportScopeID(replayTaskID, inst)
	m.reportMu.Lock()
	defer m.reportMu.Unlock()
	report := m.replayReportLocked(replayTaskID, inst)
	if err := m.saveReplayReportLocked(report); err != nil {
		m.setError(err)
	}
}

func (m *Manager) replayReportLocked(replayTaskID string, inst StrategyInstance) *ReplayReport {
	if m.reports == nil {
		m.reports = make(map[string]*ReplayReport)
	}
	key := replayReportKey(replayTaskID, inst.InstanceID)
	if existing := m.reports[key]; existing != nil {
		return existing
	}
	now := time.Now()
	report := &ReplayReport{
		RunID:        replayReportRunID(replayTaskID, inst.InstanceID),
		ReplayTaskID: strings.TrimSpace(replayTaskID),
		InstanceID:   inst.InstanceID,
		StrategyID:   inst.StrategyID,
		DisplayName:  firstNonEmpty(inst.DisplayName, inst.InstanceID),
		Symbol:       firstSymbol(inst.Symbols),
		Timeframe:    inst.Timeframe,
		Status:       "running",
		StartedAt:    now,
	}
	m.reports[key] = report
	return report
}

func (m *Manager) saveReplayReportLocked(report *ReplayReport) error {
	run := StrategyRun{
		RunID:      report.RunID,
		InstanceID: report.InstanceID,
		StrategyID: report.StrategyID,
		RunType:    RunTypeReplayReport,
		Status:     report.Status,
		Symbol:     report.Symbol,
		Timeframe:  report.Timeframe,
		Summary:    report.summary(),
		StartedAt:  report.StartedAt,
		FinishedAt: report.FinishedAt,
	}
	outputPath, err := m.writeBacktestOutput(run, map[string]any{
		"replay_task_id": report.ReplayTaskID,
		"instance_id":    report.InstanceID,
		"strategy_id":    report.StrategyID,
		"symbol":         report.Symbol,
		"timeframe":      report.Timeframe,
	}, BacktestResponse{RunID: report.RunID, Status: report.Status, Summary: report.summary(), Result: report.result()})
	if err != nil {
		return err
	}
	run.OutputPath = outputPath
	if err := m.store.SaveRun(run); err != nil {
		return err
	}
	m.broadcast("strategy_backtest_done", run)
	return nil
}

func (m *Manager) FinalizeReplayReports(replayTaskID string, status string) {
	if m == nil || m.store == nil || strings.TrimSpace(replayTaskID) == "" {
		return
	}
	finalStatus := "done"
	if strings.EqualFold(strings.TrimSpace(status), "stopped") {
		finalStatus = "stopped"
	}
	m.reportMu.Lock()
	defer m.reportMu.Unlock()
	finished := time.Now()
	for _, report := range m.reports {
		if report == nil || report.ReplayTaskID != replayTaskID || report.Status != "running" {
			continue
		}
		report.Status = finalStatus
		report.FinishedAt = &finished
		if err := m.saveReplayReportLocked(report); err != nil {
			m.setError(err)
		}
	}
}

func (m *Manager) FinalizeReplayReportsForInstance(instanceID string, status string) {
	if m == nil || m.store == nil || strings.TrimSpace(instanceID) == "" {
		return
	}
	finalStatus := "done"
	if strings.EqualFold(strings.TrimSpace(status), "stopped") {
		finalStatus = "stopped"
	}
	m.reportMu.Lock()
	defer m.reportMu.Unlock()
	finished := time.Now()
	for _, report := range m.reports {
		if report == nil || report.InstanceID != instanceID || report.Status != "running" {
			continue
		}
		report.Status = finalStatus
		report.FinishedAt = &finished
		if err := m.saveReplayReportLocked(report); err != nil {
			m.setError(err)
		}
	}
}

func replayReportScopeID(taskID string, inst StrategyInstance) string {
	taskID = strings.TrimSpace(taskID)
	if taskID != "" {
		return taskID
	}
	return "instance-" + strings.TrimSpace(inst.InstanceID)
}

func replayReportKey(taskID string, instanceID string) string {
	return strings.TrimSpace(taskID) + "|" + strings.TrimSpace(instanceID)
}

func replayReportRunID(taskID string, instanceID string) string {
	return fmt.Sprintf("replay-report-%s-%s", safeRunIDPart(taskID), safeRunIDPart(instanceID))
}

func safeRunIDPart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

func normalizeZero(v float64) float64 {
	if math.Abs(v) < 1e-9 {
		return 0
	}
	return v
}
