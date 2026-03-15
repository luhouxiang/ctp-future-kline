package strategy

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	dbx "ctp-go-demo/internal/db"
)

type Store struct {
	db *sql.DB
}

func NewStore(dsn string) (*Store, error) {
	db, err := dbx.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) UpsertDefinition(def StrategyDefinition) error {
	params, err := json.Marshal(def.DefaultParams)
	if err != nil {
		return err
	}
	if def.UpdatedAt.IsZero() {
		def.UpdatedAt = time.Now()
	}
	_, err = s.db.Exec(`
INSERT INTO strategy_definitions(strategy_id,display_name,entry_script,version,default_params_json,updated_at)
VALUES(?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
display_name=VALUES(display_name),
entry_script=VALUES(entry_script),
version=VALUES(version),
default_params_json=VALUES(default_params_json),
updated_at=VALUES(updated_at)
`, def.StrategyID, def.DisplayName, def.EntryScript, def.Version, string(params), def.UpdatedAt)
	return err
}

func (s *Store) ListDefinitions() ([]StrategyDefinition, error) {
	rows, err := s.db.Query(`SELECT strategy_id,display_name,entry_script,version,default_params_json,updated_at FROM strategy_definitions ORDER BY strategy_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]StrategyDefinition, 0, 16)
	for rows.Next() {
		var def StrategyDefinition
		var raw string
		if err := rows.Scan(&def.StrategyID, &def.DisplayName, &def.EntryScript, &def.Version, &raw, &def.UpdatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(raw), &def.DefaultParams)
		out = append(out, def)
	}
	return out, rows.Err()
}

func (s *Store) SaveInstance(inst StrategyInstance) error {
	symbols, err := json.Marshal(inst.Symbols)
	if err != nil {
		return err
	}
	params, err := json.Marshal(inst.Params)
	if err != nil {
		return err
	}
	now := time.Now()
	if inst.CreatedAt.IsZero() {
		inst.CreatedAt = now
	}
	inst.UpdatedAt = now
	_, err = s.db.Exec(`
INSERT INTO strategy_instances(instance_id,strategy_id,display_name,mode,status,account_id,symbols_json,timeframe,params_json,last_signal_at,last_target_position,last_error,updated_at,created_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
strategy_id=VALUES(strategy_id),
display_name=VALUES(display_name),
mode=VALUES(mode),
status=VALUES(status),
account_id=VALUES(account_id),
symbols_json=VALUES(symbols_json),
timeframe=VALUES(timeframe),
params_json=VALUES(params_json),
last_signal_at=VALUES(last_signal_at),
last_target_position=VALUES(last_target_position),
last_error=VALUES(last_error),
updated_at=VALUES(updated_at)
`, inst.InstanceID, inst.StrategyID, inst.DisplayName, inst.Mode, inst.Status, inst.AccountID, string(symbols), inst.Timeframe, string(params), inst.LastSignalAt, inst.LastTargetPosition, inst.LastError, inst.UpdatedAt, inst.CreatedAt)
	return err
}

func (s *Store) ListInstances() ([]StrategyInstance, error) {
	rows, err := s.db.Query(`SELECT instance_id,strategy_id,display_name,mode,status,account_id,symbols_json,timeframe,params_json,last_signal_at,last_target_position,last_error,updated_at,created_at FROM strategy_instances ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StrategyInstance
	for rows.Next() {
		var item StrategyInstance
		var symbolsRaw string
		var paramsRaw string
		var lastSignal sql.NullTime
		var lastError sql.NullString
		if err := rows.Scan(&item.InstanceID, &item.StrategyID, &item.DisplayName, &item.Mode, &item.Status, &item.AccountID, &symbolsRaw, &item.Timeframe, &paramsRaw, &lastSignal, &item.LastTargetPosition, &lastError, &item.UpdatedAt, &item.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(symbolsRaw), &item.Symbols)
		_ = json.Unmarshal([]byte(paramsRaw), &item.Params)
		if lastSignal.Valid {
			ts := lastSignal.Time
			item.LastSignalAt = &ts
		}
		if lastError.Valid {
			item.LastError = lastError.String
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) GetInstance(instanceID string) (StrategyInstance, error) {
	var item StrategyInstance
	var symbolsRaw string
	var paramsRaw string
	var lastSignal sql.NullTime
	var lastError sql.NullString
	err := s.db.QueryRow(`SELECT instance_id,strategy_id,display_name,mode,status,account_id,symbols_json,timeframe,params_json,last_signal_at,last_target_position,last_error,updated_at,created_at FROM strategy_instances WHERE instance_id=?`, instanceID).
		Scan(&item.InstanceID, &item.StrategyID, &item.DisplayName, &item.Mode, &item.Status, &item.AccountID, &symbolsRaw, &item.Timeframe, &paramsRaw, &lastSignal, &item.LastTargetPosition, &lastError, &item.UpdatedAt, &item.CreatedAt)
	if err != nil {
		return item, err
	}
	_ = json.Unmarshal([]byte(symbolsRaw), &item.Symbols)
	_ = json.Unmarshal([]byte(paramsRaw), &item.Params)
	if lastSignal.Valid {
		ts := lastSignal.Time
		item.LastSignalAt = &ts
	}
	if lastError.Valid {
		item.LastError = lastError.String
	}
	return item, nil
}

func (s *Store) AppendSignal(sig SignalRecord) (int64, error) {
	metrics, err := json.Marshal(sig.Metrics)
	if err != nil {
		return 0, err
	}
	if sig.CreatedAt.IsZero() {
		sig.CreatedAt = time.Now()
	}
	res, err := s.db.Exec(`
INSERT INTO strategy_signals(instance_id,strategy_id,symbol,timeframe,mode,event_time,target_position,confidence,reason,metrics_json,created_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?)
`, sig.InstanceID, sig.StrategyID, sig.Symbol, sig.Timeframe, sig.Mode, sig.EventTime, sig.TargetPosition, sig.Confidence, sig.Reason, string(metrics), sig.CreatedAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) ListSignals(limit int) ([]SignalRecord, error) {
	rows, err := s.db.Query(`SELECT id,instance_id,strategy_id,symbol,timeframe,mode,event_time,target_position,confidence,reason,metrics_json,created_at FROM strategy_signals ORDER BY event_time DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SignalRecord
	for rows.Next() {
		var sig SignalRecord
		var raw string
		if err := rows.Scan(&sig.ID, &sig.InstanceID, &sig.StrategyID, &sig.Symbol, &sig.Timeframe, &sig.Mode, &sig.EventTime, &sig.TargetPosition, &sig.Confidence, &sig.Reason, &raw, &sig.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(raw), &sig.Metrics)
		out = append(out, sig)
	}
	return out, rows.Err()
}

func (s *Store) SaveRun(run StrategyRun) error {
	summary, err := json.Marshal(run.Summary)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
INSERT INTO strategy_runs(run_id,instance_id,strategy_id,run_type,status,symbol,timeframe,output_path,summary_json,started_at,finished_at,last_error)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
status=VALUES(status),
output_path=VALUES(output_path),
summary_json=VALUES(summary_json),
finished_at=VALUES(finished_at),
last_error=VALUES(last_error)
`, run.RunID, run.InstanceID, run.StrategyID, run.RunType, run.Status, run.Symbol, run.Timeframe, run.OutputPath, string(summary), run.StartedAt, run.FinishedAt, run.LastError)
	return err
}

func (s *Store) ListRuns(limit int) ([]StrategyRun, error) {
	rows, err := s.db.Query(`SELECT run_id,instance_id,strategy_id,run_type,status,symbol,timeframe,output_path,summary_json,started_at,finished_at,last_error FROM strategy_runs ORDER BY started_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StrategyRun
	for rows.Next() {
		var run StrategyRun
		var raw string
		var finished sql.NullTime
		var lastError sql.NullString
		if err := rows.Scan(&run.RunID, &run.InstanceID, &run.StrategyID, &run.RunType, &run.Status, &run.Symbol, &run.Timeframe, &run.OutputPath, &raw, &run.StartedAt, &finished, &lastError); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(raw), &run.Summary)
		if finished.Valid {
			ts := finished.Time
			run.FinishedAt = &ts
		}
		if lastError.Valid {
			run.LastError = lastError.String
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (s *Store) GetRun(runID string) (StrategyRun, error) {
	var run StrategyRun
	var raw string
	var finished sql.NullTime
	var lastError sql.NullString
	err := s.db.QueryRow(`SELECT run_id,instance_id,strategy_id,run_type,status,symbol,timeframe,output_path,summary_json,started_at,finished_at,last_error FROM strategy_runs WHERE run_id=?`, runID).
		Scan(&run.RunID, &run.InstanceID, &run.StrategyID, &run.RunType, &run.Status, &run.Symbol, &run.Timeframe, &run.OutputPath, &raw, &run.StartedAt, &finished, &lastError)
	if err != nil {
		return run, err
	}
	_ = json.Unmarshal([]byte(raw), &run.Summary)
	if finished.Valid {
		ts := finished.Time
		run.FinishedAt = &ts
	}
	if lastError.Valid {
		run.LastError = lastError.String
	}
	return run, nil
}

func (s *Store) AppendOrderAudit(rec OrderAuditRecord) (int64, error) {
	raw, err := json.Marshal(rec.Audit)
	if err != nil {
		return 0, err
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now()
	}
	res, err := s.db.Exec(`
INSERT INTO order_audit_logs(instance_id,strategy_id,symbol,mode,event_time,target_position,current_position,planned_delta,risk_status,risk_reason,order_status,audit_json,created_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
`, rec.InstanceID, rec.StrategyID, rec.Symbol, rec.Mode, rec.EventTime, rec.TargetPosition, rec.CurrentPosition, rec.PlannedDelta, rec.RiskStatus, rec.RiskReason, rec.OrderStatus, string(raw), rec.CreatedAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) ListOrderAudits(limit int) ([]OrderAuditRecord, error) {
	rows, err := s.db.Query(`SELECT id,instance_id,strategy_id,symbol,mode,event_time,target_position,current_position,planned_delta,risk_status,risk_reason,order_status,audit_json,created_at FROM order_audit_logs ORDER BY event_time DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OrderAuditRecord
	for rows.Next() {
		var rec OrderAuditRecord
		var raw string
		if err := rows.Scan(&rec.ID, &rec.InstanceID, &rec.StrategyID, &rec.Symbol, &rec.Mode, &rec.EventTime, &rec.TargetPosition, &rec.CurrentPosition, &rec.PlannedDelta, &rec.RiskStatus, &rec.RiskReason, &rec.OrderStatus, &raw, &rec.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(raw), &rec.Audit)
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *Store) Counts() (definitions int, instances int, signals int64, audits int64, runs int64, err error) {
	if err = s.db.QueryRow(`SELECT COUNT(1) FROM strategy_definitions`).Scan(&definitions); err != nil {
		return
	}
	if err = s.db.QueryRow(`SELECT COUNT(1) FROM strategy_instances`).Scan(&instances); err != nil {
		return
	}
	if err = s.db.QueryRow(`SELECT COUNT(1) FROM strategy_signals`).Scan(&signals); err != nil {
		return
	}
	if err = s.db.QueryRow(`SELECT COUNT(1) FROM order_audit_logs`).Scan(&audits); err != nil {
		return
	}
	if err = s.db.QueryRow(`SELECT COUNT(1) FROM strategy_runs`).Scan(&runs); err != nil {
		return
	}
	return
}

func mustRunID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
