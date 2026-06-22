package strategy

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteStrategyRunArchiveCreatesMatchingJSONAndCSV(t *testing.T) {
	dir := t.TempDir()
	run := StrategyRun{
		RunID:      "replay/report:1",
		InstanceID: "inst-1",
		StrategyID: "demo.strategy",
		RunType:    RunTypeReplayReport,
		Status:     "done",
		Symbol:     "ao2609",
		Timeframe:  "5m",
		StartedAt:  time.Now(),
	}
	resp := BacktestResponse{
		RunID:  run.RunID,
		Status: "done",
		Result: map[string]any{
			"signal_table": []any{
				map[string]any{"event_time": "2026-04-02T23:00:00+08:00", "target_position": -1, "reason": "entry"},
			},
		},
	}
	jsonPath, csvPath, err := writeStrategyRunArchive(dir, run, map[string]any{"note": "request"}, resp)
	if err != nil {
		t.Fatalf("writeStrategyRunArchive() error = %v", err)
	}
	if filepath.Base(jsonPath) != filepath.Base(strings.TrimSuffix(csvPath, ".csv")+".json") {
		t.Fatalf("json/csv base mismatch: json=%s csv=%s", jsonPath, csvPath)
	}
	if !fileExists(jsonPath) || !fileExists(csvPath) {
		t.Fatalf("archive files missing: json=%v csv=%v", fileExists(jsonPath), fileExists(csvPath))
	}
	body, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	var archived map[string]any
	if err := json.Unmarshal(body, &archived); err != nil {
		t.Fatalf("parse json: %v", err)
	}
	if archived["type"] != "strategy_run" || archived["result"] == nil || archived["request"] == nil {
		t.Fatalf("archive json missing expected fields: %+v", archived)
	}
	csvBody, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if !bytes.HasPrefix(csvBody, []byte{0xEF, 0xBB, 0xBF}) {
		t.Fatalf("csv missing UTF-8 BOM")
	}
	rows, err := csv.NewReader(bytes.NewReader(bytes.TrimPrefix(csvBody, []byte{0xEF, 0xBB, 0xBF}))).ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	if len(rows) < 2 || len(rows[0]) != len(rows[1]) {
		t.Fatalf("csv header/data column mismatch: rows=%#v", rows)
	}
	if !strings.Contains(string(csvBody), "时间") || !strings.Contains(string(csvBody), "买卖") || !strings.Contains(string(csvBody), "entry") {
		t.Fatalf("csv missing trade event rows:\n%s", string(csvBody))
	}
}

func TestWriteStrategyInstanceArchiveCreatesStrategyJSON(t *testing.T) {
	dir := t.TempDir()
	inst := StrategyInstance{
		InstanceID: "combo/demo:1",
		StrategyID: "ma20.state_diagram_short",
		Symbols:    []string{"ao2609"},
		Timeframe:  "5m",
		Params:     map[string]any{"feature_dependencies": []string{"zigzag_atr26"}},
	}
	path, err := writeStrategyInstanceArchive(dir, inst)
	if err != nil {
		t.Fatalf("writeStrategyInstanceArchive() error = %v", err)
	}
	if !strings.HasPrefix(filepath.Base(path), strategyArchiveInstancePrefix) || !strings.HasSuffix(path, ".json") {
		t.Fatalf("unexpected archive path: %s", path)
	}
	if !fileExists(path) {
		t.Fatalf("instance archive missing: %s", path)
	}
}

func TestComboArchiveMergesInstancesAndRuns(t *testing.T) {
	dir := t.TempDir()
	helper := StrategyInstance{
		InstanceID: "combo-ma20_state_zigzag-helper-indicator.zigzag_atr26-ao2609-5m-1782020239502",
		StrategyID: "indicator.zigzag_atr26",
		Symbols:    []string{"ao2609"},
		Timeframe:  "5m",
		Params: map[string]any{
			"composition_id":   "ma20_state_zigzag",
			"composition_role": "helper",
		},
	}
	primary := StrategyInstance{
		InstanceID: "combo-ma20_state_zigzag-primary-ao2609-5m-1782020239502",
		StrategyID: "ma20.state_diagram_short",
		Symbols:    []string{"ao2609"},
		Timeframe:  "5m",
		Params: map[string]any{
			"composition_id":   "ma20_state_zigzag",
			"composition_role": "primary",
		},
	}
	helperPath, err := writeStrategyInstanceArchive(dir, helper)
	if err != nil {
		t.Fatalf("write helper archive: %v", err)
	}
	primaryPath, err := writeStrategyInstanceArchive(dir, primary)
	if err != nil {
		t.Fatalf("write primary archive: %v", err)
	}
	if helperPath != primaryPath {
		t.Fatalf("combo instance archive path mismatch: helper=%s primary=%s", helperPath, primaryPath)
	}
	body, err := os.ReadFile(primaryPath)
	if err != nil {
		t.Fatalf("read combo strategy json: %v", err)
	}
	if !strings.Contains(string(body), `"primary"`) || !strings.Contains(string(body), `"helpers"`) {
		t.Fatalf("combo strategy json missing primary/helpers:\n%s", string(body))
	}

	helperRunID := "replay-report-task-1-" + strings.ReplaceAll(helper.InstanceID, ".", "_")
	helperRun := StrategyRun{RunID: helperRunID, InstanceID: helper.InstanceID, StrategyID: helper.StrategyID, RunType: RunTypeReplayReport, Status: "done", Symbol: "ao2609", Timeframe: "5m", StartedAt: time.Now()}
	primaryRun := StrategyRun{RunID: "replay-report-task-1-" + primary.InstanceID, InstanceID: primary.InstanceID, StrategyID: primary.StrategyID, RunType: RunTypeReplayReport, Status: "done", Symbol: "ao2609", Timeframe: "5m", StartedAt: time.Now()}
	helperJSON, helperCSV, err := writeStrategyRunArchive(dir, helperRun, map[string]any{"role": "helper"}, BacktestResponse{Result: map[string]any{"signal_table": []any{map[string]any{"reason": "zigzag"}}}})
	if err != nil {
		t.Fatalf("write helper run archive: %v", err)
	}
	primaryJSON, primaryCSV, err := writeStrategyRunArchive(dir, primaryRun, map[string]any{"role": "primary"}, BacktestResponse{Result: map[string]any{"signal_table": []any{map[string]any{"reason": "short"}}}})
	if err != nil {
		t.Fatalf("write primary run archive: %v", err)
	}
	if helperJSON != primaryJSON || helperCSV != primaryCSV {
		t.Fatalf("combo run archive path mismatch: helper=%s/%s primary=%s/%s", helperJSON, helperCSV, primaryJSON, primaryCSV)
	}
	runBody, err := os.ReadFile(primaryJSON)
	if err != nil {
		t.Fatalf("read combo run json: %v", err)
	}
	if !strings.Contains(string(runBody), helperRun.RunID) || !strings.Contains(string(runBody), primaryRun.RunID) {
		t.Fatalf("combo run json missing both runs:\n%s", string(runBody))
	}
	csvBody, err := os.ReadFile(primaryCSV)
	if err != nil {
		t.Fatalf("read combo run csv: %v", err)
	}
	if !strings.Contains(string(csvBody), "helper") || !strings.Contains(string(csvBody), "primary") {
		t.Fatalf("combo csv missing role rows:\n%s", string(csvBody))
	}
}
