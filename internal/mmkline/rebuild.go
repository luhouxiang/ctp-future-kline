package mmkline

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/klineagg"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/sessiontime"
)

const (
	colInstrumentID = "InstrumentID"
	colExchange     = "Exchange"
	colDataTime     = "DataTime"
	colAdjustedTime = "AdjustedTime"
	colUpdateTime   = "UpdateTime"
	colPeriod       = "Period"
	colOpen         = "Open"
	colHigh         = "High"
	colLow          = "Low"
	colClose        = "Close"
	colVolume       = "Volume"
	colOpenInterest = "OpenInterest"
	colSettlement   = "SettlementPrice"
)

// RebuildRequest 描述一次 mm 重建任务。
// 它既可以针对普通合约，也可以针对某个品种的 L9。
type RebuildRequest struct {
	Variety      string
	InstrumentID string
	IsL9         bool
	// Periods 可选指定重建周期（5m/15m/30m/1h/1d）；为空表示全部。
	Periods []string
}

// SessionRangeJSON 是交易时段配置落到 JSON 时使用的结构。
type SessionRangeJSON struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

var ErrTradingSessionNotReady = fmt.Errorf("trading session not completed")

// RebuildAndUpsert 根据 1m 源表重建多周期 mm 数据并写回目标表。
//
// 处理步骤概括如下：
// 1. 依据 IsL9 选择普通合约源表或 L9 源表
// 2. 加载已完成的交易时段配置，必要时尝试推断并写回
// 3. 读取该合约或 L9 的全部 1m bar
// 4. 按交易时段与周期桶重新聚合
// 5. 把生成结果回写到 mm 表
//
// 当前实现是“以目标合约/目标 L9 为单位进行重建”，不是只增量更新单个桶。
func RebuildAndUpsert(db *sql.DB, req RebuildRequest) (map[string]int, []klineagg.BucketStat, error) {
	return RebuildAndUpsertWithSessionDB(db, db, req, true)
}

func RebuildAndUpsertWithSessionDB(db *sql.DB, sessionDB *sql.DB, req RebuildRequest, allowInfer bool) (map[string]int, []klineagg.BucketStat, error) {
	if db == nil {
		return nil, nil, fmt.Errorf("nil db")
	}
	if sessionDB == nil {
		sessionDB = db
	}
	variety := normalizeVariety(req.Variety)
	if variety == "" {
		return nil, nil, fmt.Errorf("invalid variety: %q", req.Variety)
	}
	instrumentID := strings.ToLower(strings.TrimSpace(req.InstrumentID))
	if instrumentID == "" {
		return nil, nil, fmt.Errorf("invalid instrument id")
	}
	srcTable, err := sourceTableName(variety, req.IsL9)
	if err != nil {
		return nil, nil, err
	}
	sessions, err := loadCompletedTradingSessions(sessionDB, variety)
	if err != nil {
		if err == ErrTradingSessionNotReady && allowInfer {
			if inferErr := inferAndUpsertTradingSessions(sessionDB, db, srcTable, instrumentID, variety); inferErr != nil {
				return nil, nil, inferErr
			}
			sessions, err = loadCompletedTradingSessions(sessionDB, variety)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	dstTable, err := mmTableName(variety, req.IsL9)
	if err != nil {
		return nil, nil, err
	}
	bars, err := queryAllMinuteBars(db, srcTable, instrumentID)
	if err != nil {
		return nil, nil, err
	}
	if len(bars) == 0 {
		return map[string]int{}, nil, nil
	}
	if err := ensureMMKlineTable(db, dstTable); err != nil {
		return nil, nil, err
	}

	periods := []struct {
		Label   string
		Minutes int
	}{
		{Label: "5m", Minutes: 5},
		{Label: "15m", Minutes: 15},
		{Label: "30m", Minutes: 30},
		{Label: "1h", Minutes: 60},
		{Label: "1d", Minutes: 1440},
	}
	selected := selectedPeriods(req.Periods)
	if len(selected) > 0 {
		filtered := make([]struct {
			Label   string
			Minutes int
		}, 0, len(periods))
		for _, p := range periods {
			if selected[p.Label] {
				filtered = append(filtered, p)
			}
		}
		periods = filtered
	}

	written := make(map[string]int, len(periods))
	var allStats []klineagg.BucketStat
	for _, p := range periods {
		if p.Label == "1d" {
			out, stats := aggregateToDaily(bars, p.Label, sessions)
			if len(out) == 0 {
				continue
			}
			if err := upsertMMBars(db, dstTable, out); err != nil {
				return nil, nil, err
			}
			written[p.Label] = len(out)
			allStats = append(allStats, stats...)
			continue
		}
		out, stats := klineagg.Aggregate(bars, sessions, p.Label, p.Minutes, klineagg.Options{CrossSessionFor30m1h: true, ClampToSessionEnd: true, ComputeBucketStats: true})
		if len(out) == 0 {
			continue
		}
		if err := upsertMMBars(db, dstTable, out); err != nil {
			return nil, nil, err
		}
		written[p.Label] = len(out)
		allStats = append(allStats, stats...)
	}
	return written, allStats, nil
}

func selectedPeriods(items []string) map[string]bool {
	if len(items) == 0 {
		return nil
	}
	out := make(map[string]bool, len(items))
	for _, item := range items {
		v := strings.ToLower(strings.TrimSpace(item))
		switch v {
		case "5m", "15m", "30m", "1h", "1d":
			out[v] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func InferAndUpsertTradingSessions(sessionDB *sql.DB, sourceDB *sql.DB, req RebuildRequest) error {
	if sourceDB == nil {
		return fmt.Errorf("nil source db")
	}
	if sessionDB == nil {
		sessionDB = sourceDB
	}
	variety := normalizeVariety(req.Variety)
	if variety == "" {
		return fmt.Errorf("invalid variety: %q", req.Variety)
	}
	instrumentID := strings.ToLower(strings.TrimSpace(req.InstrumentID))
	if instrumentID == "" {
		return fmt.Errorf("invalid instrument id")
	}
	srcTable, err := sourceTableName(variety, req.IsL9)
	if err != nil {
		return err
	}
	return inferAndUpsertTradingSessions(sessionDB, sourceDB, srcTable, instrumentID, variety)
}

func aggregateToDaily(bars []klineagg.MinuteBar, period string, sessions []klineagg.SessionRange) ([]klineagg.AggBar, []klineagg.BucketStat) {
	if len(bars) == 0 {
		return nil, nil
	}
	type dayKey struct {
		day string
	}
	pos := make(map[dayKey]int, 16)
	out := make([]klineagg.AggBar, 0, len(bars)/240+8)
	for _, b := range bars {
		if b.DataTime.IsZero() {
			continue
		}
		plan, ok := klineagg.PlanBucket(b, sessions, period, 1440)
		if !ok {
			continue
		}
		k := dayKey{day: plan.TradingDay}
		idx, ok := pos[k]
		if !ok {
			pos[k] = len(out)
			out = append(out, klineagg.AggBar{
				InstrumentID: b.InstrumentID,
				Exchange:     b.Exchange,
				DataTime:     plan.DataTime,
				AdjustedTime: plan.AdjustedTime,
				Period:       period,
				Open:         b.Open,
				High:         b.High,
				Low:          b.Low,
				Close:        b.Close,
				Volume:       b.Volume,
				OpenInterest: b.OpenInterest,
			})
			continue
		}
		a := out[idx]
		if b.High > a.High {
			a.High = b.High
		}
		if b.Low < a.Low {
			a.Low = b.Low
		}
		a.Close = b.Close
		a.Volume += b.Volume
		a.OpenInterest = b.OpenInterest
		out[idx] = a
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AdjustedTime.Before(out[j].AdjustedTime) })
	return out, nil
}

func loadCompletedTradingSessions(db *sql.DB, variety string) ([]klineagg.SessionRange, error) {
	var (
		raw       string
		completed bool
	)
	dbName, dbErr := dbx.CurrentDatabase(db)
	if dbErr != nil {
		dbName = "<unknown>"
	}
	err := db.QueryRow(`SELECT session_json,is_completed FROM trading_sessions WHERE variety=?`, variety).Scan(&raw, &completed)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Debug("trading sessions resolved", "variety", variety, "database", dbName, "source", "default", "reason", "no_rows", "session_text", sessiontime.DefaultSessionText)
			return defaultKlineAggSessions(), nil
		}
		return nil, fmt.Errorf("query trading_sessions failed: %w", err)
	}
	if !completed {
		logger.Debug("trading sessions resolved", "variety", variety, "database", dbName, "source", "default", "reason", "not_completed", "session_text", sessiontime.DefaultSessionText)
		return defaultKlineAggSessions(), nil
	}
	sessionText, err := sessiontime.SessionJSONText(raw)
	if err != nil {
		return nil, err
	}
	logger.Debug("trading sessions resolved", "variety", variety, "database", dbName, "source", "db", "session_text", sessionText, "is_completed", completed)
	return parseSessionJSON(raw)
}

func defaultKlineAggSessions() []klineagg.SessionRange {
	src := sessiontime.DefaultRanges()
	out := make([]klineagg.SessionRange, 0, len(src))
	for _, r := range src {
		out = append(out, klineagg.SessionRange{Start: r.Start, End: r.End})
	}
	return out
}

func inferAndUpsertTradingSessions(sessionDB *sql.DB, sourceDB *sql.DB, srcTable, instrumentID, variety string) error {
	recent5, err := queryRecent5DataDays(sourceDB, srcTable, instrumentID)
	if err != nil {
		return err
	}
	if len(recent5) < 3 {
		return fmt.Errorf("trading session rebuild failed: not enough days for variety=%s, got=%d", variety, len(recent5))
	}
	middle3 := recent5
	if len(recent5) >= 5 {
		middle3 = []string{recent5[1], recent5[2], recent5[3]}
	} else {
		// Fallback for sparse history: use the latest 3 available days.
		middle3 = recent5[len(recent5)-3:]
	}
	sets := make([]map[int]struct{}, 0, 3)
	for _, day := range middle3 {
		set, err := queryDayMinuteSetFloor5(sourceDB, srcTable, instrumentID, day)
		if err != nil {
			return err
		}
		sets = append(sets, set)
	}
	// Progressive fallback to avoid dropping night session:
	// 1) intersection, 2) majority(>=2/3), 3) union.
	selected := intersectMinuteSets(sets)
	if !hasNightMinutes(selected) {
		selected = majorityMinuteSet(sets, 2)
	}
	if !hasNightMinutes(selected) {
		selected = unionMinuteSets(sets)
	}
	if len(selected) == 0 {
		return fmt.Errorf("trading session rebuild failed: middle3 days have empty minute set for variety=%s", variety)
	}
	minutes := make([]int, 0, len(selected))
	for m := range selected {
		minutes = append(minutes, m)
	}
	sort.Slice(minutes, func(i, j int) bool { return orderKey(minutes[i]) < orderKey(minutes[j]) })
	sessions := compressMinutesToRanges(minutes)
	sessionText, sessionJSON, err := encodeTradingSessions(sessions)
	if err != nil {
		return err
	}
	sqlText := `
REPLACE INTO trading_sessions(variety,session_text,session_json,is_completed,sample_trade_date,validated_trade_date,match_ratio,updated_at)
VALUES(?,?,?,?,?,?,?,?)
`
	args := []any{
		variety,
		sessionText,
		sessionJSON,
		true,
		middle3[0],
		middle3[2],
		1.0,
		time.Now().Format("2006-01-02 15:04:05"),
	}
	dbName, dbErr := dbx.CurrentDatabase(sessionDB)
	if dbErr != nil {
		dbName = "<unknown>"
	}
	logger.Warn("trading_sessions exec",
		"database", dbName,
		"table", "trading_sessions",
		"sql", strings.TrimSpace(sqlText),
		"args", fmt.Sprintf("%#v", args),
	)
	_, err = sessionDB.Exec(sqlText, args...)
	if err != nil {
		return fmt.Errorf("upsert trading_sessions failed: %w", err)
	}
	logger.Warn("trading_sessions exec success",
		"database", dbName,
		"table", "trading_sessions",
		"variety", variety,
	)
	logger.Info("kline pipeline",
		"stage", "session_infer_result",
		"variety", variety,
		"session_text", sessionText,
		"middle3_days", middle3,
	)
	return nil
}

func parseSessionJSON(raw string) ([]klineagg.SessionRange, error) {
	var rows []SessionRangeJSON
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		return nil, fmt.Errorf("parse session_json failed: %w", err)
	}
	out := make([]klineagg.SessionRange, 0, len(rows))
	for _, row := range rows {
		start, err := parseHHMMToMinute(row.Start)
		if err != nil {
			return nil, err
		}
		end, err := parseHHMMToMinute(row.End)
		if err != nil {
			return nil, err
		}
		// Support cross-midnight session like 21:00-02:30.
		if end < start {
			out = append(out, klineagg.SessionRange{Start: start, End: 23*60 + 59})
			out = append(out, klineagg.SessionRange{Start: 0, End: end})
			continue
		}
		out = append(out, klineagg.SessionRange{Start: start, End: end})
	}
	sort.Slice(out, func(i, j int) bool {
		return orderKey(out[i].Start) < orderKey(out[j].Start)
	})
	return out, nil
}

func queryRecent5DataDays(db *sql.DB, srcTable, instrumentID string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf(`
SELECT DATE("%s") AS d
FROM "%s"
WHERE "%s"=? AND "%s"='1m'
GROUP BY DATE("%s")
ORDER BY d DESC
LIMIT 5`, colAdjustedTime, srcTable, colInstrumentID, colPeriod, colAdjustedTime), instrumentID)
	if err != nil {
		return nil, fmt.Errorf("query recent5 days failed: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d time.Time
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, d.Format("2006-01-02"))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func queryDayMinuteSetFloor5(db *sql.DB, srcTable, instrumentID, day string) (map[int]struct{}, error) {
	rows, err := db.Query(fmt.Sprintf(`
SELECT "%s"
FROM "%s"
WHERE "%s"=? AND "%s"='1m' AND DATE("%s")=?
`, colAdjustedTime, srcTable, colInstrumentID, colPeriod, colAdjustedTime), instrumentID, day)
	if err != nil {
		return nil, fmt.Errorf("query day minute set failed: %w", err)
	}
	defer rows.Close()
	out := make(map[int]struct{})
	for rows.Next() {
		var ts time.Time
		if err := rows.Scan(&ts); err != nil {
			return nil, err
		}
		m := ts.Hour()*60 + ts.Minute()
		m = (m / 5) * 5
		out[m] = struct{}{}
	}
	return out, rows.Err()
}

func minuteSetEquals(a, b map[int]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func intersectMinuteSets(sets []map[int]struct{}) map[int]struct{} {
	if len(sets) == 0 {
		return nil
	}
	out := make(map[int]struct{}, len(sets[0]))
	for k := range sets[0] {
		out[k] = struct{}{}
	}
	for i := 1; i < len(sets); i++ {
		for k := range out {
			if _, ok := sets[i][k]; !ok {
				delete(out, k)
			}
		}
	}
	return out
}

func unionMinuteSets(sets []map[int]struct{}) map[int]struct{} {
	out := make(map[int]struct{})
	for _, s := range sets {
		for k := range s {
			out[k] = struct{}{}
		}
	}
	return out
}

func majorityMinuteSet(sets []map[int]struct{}, threshold int) map[int]struct{} {
	if threshold <= 0 {
		threshold = 1
	}
	freq := make(map[int]int, 512)
	for _, s := range sets {
		for k := range s {
			freq[k]++
		}
	}
	out := make(map[int]struct{}, len(freq))
	for k, c := range freq {
		if c >= threshold {
			out[k] = struct{}{}
		}
	}
	return out
}

func hasNightMinutes(set map[int]struct{}) bool {
	for m := range set {
		if m >= 18*60 || m < 8*60 {
			return true
		}
	}
	return false
}

func compressMinutesToRanges(minutes []int) []klineagg.SessionRange {
	if len(minutes) == 0 {
		return nil
	}
	out := make([]klineagg.SessionRange, 0, 8)
	start := minutes[0]
	prev := minutes[0]
	for i := 1; i < len(minutes); i += 1 {
		m := minutes[i]
		if m == prev+5 {
			prev = m
			continue
		}
		out = append(out, klineagg.SessionRange{Start: start, End: prev})
		start = m
		prev = m
	}
	out = append(out, klineagg.SessionRange{Start: start, End: prev})
	return out
}

func encodeTradingSessions(ranges []klineagg.SessionRange) (string, string, error) {
	displayRanges := mergeContinuousAcrossMidnight(ranges)
	texts := make([]string, 0, len(displayRanges))
	jsonRanges := make([]SessionRangeJSON, 0, len(displayRanges))
	for _, r := range displayRanges {
		start := minuteToHHMM(r.Start)
		end := minuteToHHMM(r.End)
		texts = append(texts, start+"-"+end)
		jsonRanges = append(jsonRanges, SessionRangeJSON{Start: start, End: end})
	}
	raw, err := json.Marshal(jsonRanges)
	if err != nil {
		return "", "", err
	}
	return strings.Join(texts, ","), string(raw), nil
}

func mergeContinuousAcrossMidnight(ranges []klineagg.SessionRange) []klineagg.SessionRange {
	if len(ranges) <= 1 {
		return ranges
	}
	out := make([]klineagg.SessionRange, 0, len(ranges))
	i := 0
	for i < len(ranges) {
		cur := ranges[i]
		if i+1 < len(ranges) {
			next := ranges[i+1]
			// Continuous cross-midnight 5m sequence, e.g. 21:00-23:55 + 00:00-02:30 => 21:00-02:30.
			if cur.End >= 23*60+55 && next.Start == 0 {
				out = append(out, klineagg.SessionRange{Start: cur.Start, End: next.End})
				i += 2
				continue
			}
		}
		out = append(out, cur)
		i++
	}
	return out
}

func queryAllMinuteBars(db *sql.DB, srcTable, instrumentID string) ([]klineagg.MinuteBar, error) {
	rows, err := db.Query(fmt.Sprintf(`
SELECT "%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"
FROM "%s"
WHERE "%s"=? AND "%s"='1m'
ORDER BY "%s" ASC
`, colInstrumentID, colExchange, colDataTime, colAdjustedTime, colOpen, colHigh, colLow, colClose, colVolume, colOpenInterest, srcTable, colInstrumentID, colPeriod, colAdjustedTime), instrumentID)
	if err != nil {
		return nil, fmt.Errorf("query all minute bars failed: %w", err)
	}
	defer rows.Close()
	out := make([]klineagg.MinuteBar, 0, 8192)
	for rows.Next() {
		var b klineagg.MinuteBar
		if err := rows.Scan(&b.InstrumentID, &b.Exchange, &b.DataTime, &b.AdjustedTime, &b.Open, &b.High, &b.Low, &b.Close, &b.Volume, &b.OpenInterest); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func upsertMMBars(db *sql.DB, table string, bars []klineagg.AggBar) error {
	if len(bars) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	stmt := fmt.Sprintf(`
INSERT INTO "%s"
("InstrumentID","Exchange","DataTime","AdjustedTime","Period","Open","High","Low","Close","Volume","OpenInterest","SettlementPrice")
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  "AdjustedTime" = VALUES("AdjustedTime"),
  "Open" = VALUES("Open"),
  "High" = VALUES("High"),
  "Low" = VALUES("Low"),
  "Close" = VALUES("Close"),
  "Volume" = VALUES("Volume"),
  "OpenInterest" = VALUES("OpenInterest"),
  "SettlementPrice" = VALUES("SettlementPrice")
`, table)
	prep, err := tx.Prepare(stmt)
	if err != nil {
		return err
	}
	defer prep.Close()

	for _, b := range bars {
		if _, err := prep.Exec(
			strings.ToLower(strings.TrimSpace(b.InstrumentID)),
			b.Exchange,
			b.DataTime.Format("2006-01-02 15:04:00"),
			b.AdjustedTime.Format("2006-01-02 15:04:00"),
			b.Period,
			b.Open,
			b.High,
			b.Low,
			b.Close,
			b.Volume,
			b.OpenInterest,
			0.0,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func ensureMMKlineTable(db *sql.DB, table string) error {
	stmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS "%s" (
  "%s" VARCHAR(32) NOT NULL,
  "%s" VARCHAR(16) NOT NULL,
  "%s" DATETIME NOT NULL,
  "%s" DATETIME NOT NULL,
  "%s" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  "%s" VARCHAR(8) NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" BIGINT NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  PRIMARY KEY ("%s", "%s", "%s", "%s")
)`,
		table,
		colInstrumentID,
		colExchange,
		colDataTime,
		colAdjustedTime,
		colUpdateTime,
		colPeriod,
		colOpen,
		colHigh,
		colLow,
		colClose,
		colVolume,
		colOpenInterest,
		colSettlement,
		colDataTime, colInstrumentID, colExchange, colPeriod,
	)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("ensure mm kline table failed: %w", err)
	}
	if err := ensureUpdateTimeColumnMM(db, table); err != nil {
		return err
	}
	return nil
}

func ensureUpdateTimeColumnMM(db *sql.DB, table string) error {
	has, err := dbx.TableHasColumn(db, table, colUpdateTime)
	if err != nil {
		return fmt.Errorf("check update-time column failed: %w", err)
	}
	if !has {
		stmt := fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN "%s" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`,
			table, colUpdateTime)
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("add update-time column failed: %w", err)
		}
		return nil
	}
	stmt := fmt.Sprintf(`ALTER TABLE "%s" MODIFY COLUMN "%s" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`,
		table, colUpdateTime)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("modify update-time column failed: %w", err)
	}
	return nil
}

// sourceTableName 根据品种和是否为 L9 选择 1m 源表。
func sourceTableName(variety string, isL9 bool) (string, error) {
	if isL9 {
		return "future_kline_l9_1m_" + sanitizeIdent(variety), nil
	}
	return "future_kline_instrument_1m_" + sanitizeIdent(variety), nil
}

// mmTableName 根据品种和是否为 L9 选择 mm 目标表。
func mmTableName(variety string, isL9 bool) (string, error) {
	v := sanitizeIdent(variety)
	if v == "" {
		return "", fmt.Errorf("invalid variety: %q", variety)
	}
	if isL9 {
		return "future_kline_l9_mm_" + v, nil
	}
	return "future_kline_instrument_mm_" + v, nil
}

func TableNameForInstrumentMMVariety(variety string) (string, error) {
	return mmTableName(variety, false)
}

func TableNameForL9MMVariety(variety string) (string, error) {
	return mmTableName(variety, true)
}

func normalizeVariety(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func sanitizeIdent(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, ch := range strings.ToLower(strings.TrimSpace(s)) {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

func parseHHMMToMinute(v string) (int, error) {
	parts := strings.Split(strings.TrimSpace(v), ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid HH:MM: %s", v)
	}
	h, err := parseInt(parts[0])
	if err != nil {
		return 0, err
	}
	m, err := parseInt(parts[1])
	if err != nil {
		return 0, err
	}
	if h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, fmt.Errorf("invalid HH:MM: %s", v)
	}
	return h*60 + m, nil
}

func parseInt(v string) (int, error) {
	n := 0
	for _, ch := range strings.TrimSpace(v) {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid integer: %s", v)
		}
		n = n*10 + int(ch-'0')
	}
	return n, nil
}

func minuteToHHMM(minute int) string {
	if minute < 0 {
		minute = 0
	}
	h := (minute / 60) % 24
	m := minute % 60
	return fmt.Sprintf("%02d:%02d", h, m)
}

func orderKey(minute int) int {
	if minute >= 18*60 {
		return minute
	}
	return minute + 24*60
}
