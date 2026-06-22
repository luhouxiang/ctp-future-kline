package strategy

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	strategyArchiveInstancePrefix = "strategy_"
	strategyArchiveRunPrefix      = "run_"
	strategyArchiveComboPrefix    = "strategy_combo-"
	strategyArchiveComboRunPrefix = "run_combo-"
)

var strategyArchiveUnsafeName = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func strategyArchiveDir(base string) string {
	base = strings.TrimSpace(base)
	if base == "" {
		base = filepath.Join("flow", "strategy_backtests")
	}
	return base
}

func strategyArchiveStem(prefix string, id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		id = "unnamed"
	}
	id = strategyArchiveUnsafeName.ReplaceAllString(id, "_")
	id = strings.Trim(id, "._-")
	if id == "" {
		id = "unnamed"
	}
	return prefix + id
}

func strategyArchiveInstancePath(base string, instanceID string) string {
	return filepath.Join(strategyArchiveDir(base), strategyArchiveStem(strategyArchiveInstancePrefix, instanceID)+".json")
}

func strategyArchiveConfigPath(base string, inst StrategyInstance) string {
	if id := strategyArchiveComboGroupID(inst.InstanceID, inst.StrategyID, inst.Symbols, inst.Timeframe, inst.Params); id != "" {
		return filepath.Join(strategyArchiveDir(base), strategyArchiveStem(strategyArchiveComboPrefix, id)+".json")
	}
	return strategyArchiveInstancePath(base, inst.InstanceID)
}

func strategyArchiveRunJSONPath(base string, runID string) string {
	return filepath.Join(strategyArchiveDir(base), strategyArchiveStem(strategyArchiveRunPrefix, runID)+".json")
}

func strategyArchiveRunCSVPath(base string, runID string) string {
	return filepath.Join(strategyArchiveDir(base), strategyArchiveStem(strategyArchiveRunPrefix, runID)+".csv")
}

func strategyArchiveRunPaths(base string, run StrategyRun) (string, string) {
	if id := strategyArchiveComboRunGroupID(run); id != "" {
		dir := strategyArchiveDir(base)
		stem := strategyArchiveStem(strategyArchiveComboRunPrefix, id)
		return filepath.Join(dir, stem+".json"), filepath.Join(dir, stem+".csv")
	}
	return strategyArchiveRunJSONPath(base, run.RunID), strategyArchiveRunCSVPath(base, run.RunID)
}

func fileExists(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func writeStrategyInstanceArchive(base string, inst StrategyInstance) (string, error) {
	dir := strategyArchiveDir(base)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	if groupID := strategyArchiveComboGroupID(inst.InstanceID, inst.StrategyID, inst.Symbols, inst.Timeframe, inst.Params); groupID != "" {
		path := strategyArchiveConfigPath(dir, inst)
		archive := readJSONMap(path)
		archive["type"] = "strategy_combo"
		archive["composition_id"] = comboCompositionID(inst.Params)
		archive["group_id"] = groupID
		archive["symbol"] = firstNonEmpty(firstSymbol(inst.Symbols), "all")
		archive["timeframe"] = inst.Timeframe
		archive["archived_at"] = time.Now()
		role := comboRole(inst.Params, inst.InstanceID)
		if role == "primary" {
			archive["primary"] = inst
		} else {
			archive["helpers"] = upsertArchiveList(archive["helpers"], "instance_id", inst.InstanceID, inst)
		}
		archive["instances"] = upsertArchiveList(archive["instances"], "instance_id", inst.InstanceID, inst)
		if err := writeJSONFile(path, archive); err != nil {
			return "", err
		}
		return path, nil
	}
	path := strategyArchiveInstancePath(dir, inst.InstanceID)
	body, err := json.MarshalIndent(map[string]any{
		"type":        "strategy_instance",
		"instance":    inst,
		"archived_at": time.Now(),
	}, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func writeStrategyRunArchive(base string, run StrategyRun, req any, resp BacktestResponse) (string, string, error) {
	dir := strategyArchiveDir(base)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	jsonPath, csvPath := strategyArchiveRunPaths(dir, run)
	if groupID := strategyArchiveComboRunGroupID(run); groupID != "" {
		archive := readJSONMap(jsonPath)
		archive["type"] = "strategy_combo_run"
		archive["group_id"] = groupID
		archive["csv_path"] = csvPath
		archive["archived_at"] = time.Now()
		archive["runs"] = upsertArchiveList(archive["runs"], "run_id", run.RunID, run)
		archive["requests"] = upsertArchiveList(archive["requests"], "run_id", run.RunID, map[string]any{"run_id": run.RunID, "request": req})
		archive["results"] = upsertArchiveList(archive["results"], "run_id", run.RunID, map[string]any{
			"run_id":      run.RunID,
			"instance_id": run.InstanceID,
			"strategy_id": run.StrategyID,
			"role":        comboRole(nil, run.InstanceID),
			"result":      resp.Result,
		})
		if comboRole(nil, run.InstanceID) == "primary" || archive["result"] == nil {
			archive["run"] = run
			archive["request"] = req
			archive["result"] = resp.Result
		}
		if err := writeStrategyComboRunCSV(csvPath, archive["results"]); err != nil {
			return "", "", err
		}
		if err := writeJSONFile(jsonPath, archive); err != nil {
			return "", "", err
		}
		return jsonPath, csvPath, nil
	}
	if err := writeStrategyRunCSV(csvPath, resp.Result); err != nil {
		return "", "", err
	}
	body, err := json.MarshalIndent(map[string]any{
		"type":        "strategy_run",
		"run":         run,
		"request":     req,
		"result":      resp.Result,
		"csv_path":    csvPath,
		"archived_at": time.Now(),
	}, "", "  ")
	if err != nil {
		return "", "", err
	}
	if err := os.WriteFile(jsonPath, body, 0o644); err != nil {
		return "", "", err
	}
	return jsonPath, csvPath, nil
}

func writeJSONFile(path string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}

func readJSONMap(path string) map[string]any {
	body, err := os.ReadFile(path)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return map[string]any{}
	}
	if out == nil {
		return map[string]any{}
	}
	return out
}

func upsertArchiveList(raw any, key string, value string, item any) []any {
	items, _ := raw.([]any)
	out := make([]any, 0, len(items)+1)
	replaced := false
	for _, existing := range items {
		if archiveItemKey(existing, key) == value {
			out = append(out, item)
			replaced = true
			continue
		}
		out = append(out, existing)
	}
	if !replaced {
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return archiveItemKey(out[i], key) < archiveItemKey(out[j], key)
	})
	return out
}

func archiveItemKey(item any, key string) string {
	body, err := json.Marshal(item)
	if err != nil {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(m[key]))
}

func writeStrategyComboRunCSV(path string, rawResults any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := writeUTF8BOM(f); err != nil {
		return err
	}
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write(strategyTradeCSVHeader(true)); err != nil {
		return err
	}
	results, _ := rawResults.([]any)
	for _, item := range results {
		body, err := json.Marshal(item)
		if err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			continue
		}
		result, _ := m["result"].(map[string]any)
		if err := writeTradeEventRows(w, true, []string{
			strings.TrimSpace(fmt.Sprint(m["role"])),
			strings.TrimSpace(fmt.Sprint(m["instance_id"])),
			strings.TrimSpace(fmt.Sprint(m["strategy_id"])),
		}, result); err != nil {
			return err
		}
	}
	return w.Error()
}

func comboCompositionID(params map[string]any) string {
	if params == nil {
		return ""
	}
	raw, ok := params["composition_id"]
	if !ok || raw == nil {
		return ""
	}
	text := strings.TrimSpace(fmt.Sprint(raw))
	if text == "" || text == "<nil>" {
		return ""
	}
	return text
}

func comboRole(params map[string]any, instanceID string) string {
	if params != nil {
		role := strings.ToLower(strings.TrimSpace(fmt.Sprint(params["composition_role"])))
		if role == "primary" || role == "helper" {
			return role
		}
	}
	id := strings.ToLower(instanceID)
	if strings.Contains(id, "-primary-") {
		return "primary"
	}
	if strings.Contains(id, "-helper-") {
		return "helper"
	}
	return ""
}

func strategyArchiveComboGroupID(instanceID string, strategyID string, symbols []string, timeframe string, params map[string]any) string {
	compositionID := comboCompositionID(params)
	if compositionID == "" {
		return ""
	}
	symbol := firstNonEmpty(firstSymbol(symbols), "all")
	tf := firstNonEmpty(timeframe, "na")
	suffix := lastDashPart(instanceID)
	return strings.Join([]string{compositionID, symbol, tf, suffix}, "-")
}

func strategyArchiveComboRunGroupID(run StrategyRun) string {
	if !strings.HasPrefix(strings.ToLower(run.InstanceID), "combo-") {
		return ""
	}
	instanceGroup := strategyArchiveComboGroupIDFromInstanceID(run.InstanceID, run.Symbol, run.Timeframe)
	if instanceGroup == "" {
		return ""
	}
	prefix := run.RunID
	if idx := strings.Index(strings.ToLower(prefix), "-combo-"); idx >= 0 {
		prefix = prefix[:idx]
	} else {
		prefix = strings.TrimSuffix(prefix, "-"+run.InstanceID)
	}
	prefix = strings.Trim(prefix, "-")
	return strings.Join([]string{firstNonEmpty(prefix, run.RunID), instanceGroup}, "-")
}

func strategyArchiveComboGroupIDFromInstanceID(instanceID string, symbol string, timeframe string) string {
	id := strings.TrimSpace(instanceID)
	if !strings.HasPrefix(strings.ToLower(id), "combo-") {
		return ""
	}
	rest := strings.TrimPrefix(id, "combo-")
	var compositionID string
	if idx := strings.Index(rest, "-primary-"); idx >= 0 {
		compositionID = rest[:idx]
	} else if idx := strings.Index(rest, "-helper-"); idx >= 0 {
		compositionID = rest[:idx]
	} else {
		return ""
	}
	if compositionID == "" {
		return ""
	}
	return strings.Join([]string{compositionID, firstNonEmpty(symbol, "all"), firstNonEmpty(timeframe, "na"), lastDashPart(id)}, "-")
}

func lastDashPart(value string) string {
	value = strings.TrimSpace(value)
	idx := strings.LastIndex(value, "-")
	if idx < 0 || idx+1 >= len(value) {
		return "run"
	}
	return value[idx+1:]
}

func writeStrategyRunCSV(path string, result map[string]any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := writeUTF8BOM(f); err != nil {
		return err
	}
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write(strategyTradeCSVHeader(false)); err != nil {
		return err
	}
	if err := writeTradeEventRows(w, false, nil, result); err != nil {
		return err
	}
	return w.Error()
}

func strategyTradeCSVHeader(withRole bool) []string {
	header := []string{"时间", "合约", "买卖", "开平", "价格", "持仓", "盈亏", "原因", "目标", "原仓", "变化", "风控", "订单", "状态", "MA20", "MA60", "Bar"}
	if withRole {
		return append([]string{"角色", "实例", "策略"}, header...)
	}
	return header
}

func writeUTF8BOM(f *os.File) error {
	_, err := f.Write([]byte{0xEF, 0xBB, 0xBF})
	return err
}

func writeTradeEventRows(w *csv.Writer, withRole bool, prefix []string, result map[string]any) error {
	signals := archiveRows(result["signal_table"])
	orders := archiveRows(result["order_table"])
	orderByTime := make(map[string]map[string]any, len(orders))
	for _, order := range orders {
		orderByTime[csvTime(order["event_time"])] = order
	}
	if len(signals) == 0 {
		for _, order := range orders {
			if err := w.Write(strategyTradeCSVRow(withRole, prefix, order, order)); err != nil {
				return err
			}
		}
		return nil
	}
	for _, sig := range signals {
		order := orderByTime[csvTime(sig["event_time"])]
		if err := w.Write(strategyTradeCSVRow(withRole, prefix, sig, order)); err != nil {
			return err
		}
	}
	return nil
}

func strategyTradeCSVRow(withRole bool, prefix []string, sig map[string]any, order map[string]any) []string {
	metrics := archiveMap(sig["metrics"])
	if len(metrics) == 0 {
		metrics = archiveMap(archiveMap(order["audit"])["metrics"])
	}
	target := numberAny(firstNonNil(sig["target_position"], order["target_position"]))
	current := numberAny(order["current_position"])
	delta := numberAny(order["planned_delta"])
	if !hasNumber(order["planned_delta"]) {
		delta = target - current
	}
	side, offset := tradeSideOffset(current, target, delta)
	price := firstNumberString(metrics, "entry_price", "trigger_price", "signal_trigger_price", "signal_entry")
	positionAfter := formatNumberCompact(target)
	pnl := signalPnL(metrics, target)
	reason := strings.TrimSpace(fmt.Sprint(firstNonNil(sig["reason"], archiveMap(order["audit"])["reason"])))
	row := []string{
		csvTime(firstNonNil(sig["event_time"], order["event_time"])),
		strings.TrimSpace(fmt.Sprint(firstNonNil(sig["symbol"], order["symbol"]))),
		side,
		offset,
		price,
		positionAfter,
		pnl,
		reason,
		formatNumberCompact(target),
		formatNumberCompact(current),
		formatNumberCompact(delta),
		strings.TrimSpace(fmt.Sprint(order["risk_status"])),
		strings.TrimSpace(fmt.Sprint(order["order_status"])),
		strings.TrimSpace(fmt.Sprint(metrics["state"])),
		formatNumberCompact(numberAny(firstNonNil(metrics["ma20"], metrics["ma"]))),
		formatNumberCompact(numberAny(metrics["ma60"])),
		formatNumberCompact(numberAny(metrics["bar_index"])),
	}
	if withRole {
		return append(append([]string{}, prefix...), row...)
	}
	return row
}

func archiveRows(value any) []map[string]any {
	body, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var out []map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil
	}
	return out
}

func archiveMap(value any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	body, err := json.Marshal(value)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func csvTime(value any) string {
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" || text == "<nil>" {
		return ""
	}
	if ts, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return ts.Format("01/02 15:04")
	}
	return text
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func hasNumber(value any) bool {
	switch value.(type) {
	case int, int64, float64, float32, json.Number:
		return true
	default:
		_, ok := value.(string)
		if !ok {
			return false
		}
		_, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(value)), 64)
		return err == nil
	}
}

func numberAny(value any) float64 {
	switch v := value.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	case float32:
		return float64(v)
	case json.Number:
		n, _ := v.Float64()
		return n
	case string:
		n, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return n
	default:
		return 0
	}
}

func tradeSideOffset(current float64, target float64, delta float64) (string, string) {
	side := "--"
	if delta < 0 {
		side = "卖"
	} else if delta > 0 {
		side = "买"
	}
	offset := "--"
	if math.Abs(target) > math.Abs(current) {
		offset = "开"
	} else if math.Abs(target) < math.Abs(current) || target == 0 {
		offset = "平"
	}
	return side, offset
}

func firstNumberString(metrics map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := metrics[key]; ok && value != nil {
			n := numberAny(value)
			if n != 0 {
				return formatNumberCompact(n)
			}
		}
	}
	return "--"
}

func signalPnL(metrics map[string]any, target float64) string {
	result := strings.TrimSpace(fmt.Sprint(metrics["signal_result"]))
	if result == "success" {
		return "盈利"
	}
	if result == "failure" {
		return "亏损"
	}
	if target == 0 {
		return "--"
	}
	return "--"
}

func formatNumberCompact(value float64) string {
	if math.Abs(value) < 0.0000001 {
		return "0"
	}
	if math.Abs(value-math.Round(value)) < 0.0000001 {
		return fmt.Sprintf("%.0f", value)
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func writeCSVValueRows(w *csv.Writer, table string, value any) error {
	switch v := value.(type) {
	case []any:
		for i, item := range v {
			if err := writeCSVObjectRows(w, table, i, item); err != nil {
				return err
			}
		}
	case []map[string]any:
		for i, item := range v {
			if err := writeCSVObjectRows(w, table, i, item); err != nil {
				return err
			}
		}
	default:
		if err := writeCSVObjectRows(w, table, 0, v); err != nil {
			return err
		}
	}
	return nil
}

func writeCSVObjectRows(w *csv.Writer, table string, row int, value any) error {
	switch v := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := w.Write([]string{table, fmt.Sprint(row), key, csvCellString(v[key])}); err != nil {
				return err
			}
		}
	default:
		if err := w.Write([]string{table, fmt.Sprint(row), "value", csvCellString(v)}); err != nil {
			return err
		}
	}
	return nil
}

func csvCellString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		body, err := json.Marshal(v)
		if err == nil {
			return string(body)
		}
		return fmt.Sprint(v)
	}
}
