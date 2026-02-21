package klinequery

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	dbx "ctp-go-demo/internal/db"
	"ctp-go-demo/internal/logger"
	"ctp-go-demo/internal/searchindex"
)

type SearchItem struct {
	Type      string `json:"type"`
	Symbol    string `json:"symbol"`
	Variety   string `json:"variety"`
	Exchange  string `json:"exchange"`
	MinTime   string `json:"min_time"`
	MaxTime   string `json:"max_time"`
	BarCount  int64  `json:"bar_count"`
	TableName string `json:"table_name"`
}

type SearchResponse struct {
	Items    []SearchItem `json:"items"`
	Total    int          `json:"total"`
	Page     int          `json:"page"`
	PageSize int          `json:"page_size"`
}

type KlineBar struct {
	AdjustedTime int64   `json:"adjusted_time"` // 调整后的时间，即实际时间
	DataTime     int64   `json:"data_time"`     // 数据时间：交易日+数据时间
	Open         float64 `json:"open"`
	High         float64 `json:"high"`
	Low          float64 `json:"low"`
	Close        float64 `json:"close"`
	Volume       int64   `json:"volume"`
	OpenInterest float64 `json:"open_interest"`
}

type MACDPoint struct {
	Time int64    `json:"time"`
	DIF  *float64 `json:"dif"`
	DEA  *float64 `json:"dea"`
	Hist *float64 `json:"hist"`
}

type BarsMeta struct {
	Symbol   string `json:"symbol"`
	Type     string `json:"type"`
	Variety  string `json:"variety"`
	Exchange string `json:"exchange"`
}

type BarsResponse struct {
	Meta BarsMeta    `json:"meta"`
	Bars []KlineBar  `json:"bars"`
	MACD []MACDPoint `json:"macd"`
}

type Service struct {
	dbPath string
	index  *searchindex.Manager
}

func NewService(dbPath string, index *searchindex.Manager) *Service {
	return &Service{dbPath: dbPath, index: index}
}

func (s *Service) Search(keyword string, start time.Time, end time.Time, page int, pageSize int) (SearchResponse, error) {
	items, total, err := s.index.Search(keyword, start, end, page, pageSize)
	if err != nil {
		return SearchResponse{}, err
	}
	out := make([]SearchItem, 0, len(items))
	for _, it := range items {
		out = append(out, SearchItem{
			Type:      it.Kind,
			Symbol:    displaySymbol(it.Symbol, it.Kind),
			Variety:   it.Variety,
			Exchange:  it.Exchange,
			MinTime:   it.MinTime.Format("2006-01-02 15:04"),
			MaxTime:   it.MaxTime.Format("2006-01-02 15:04"),
			BarCount:  it.BarCount,
			TableName: it.TableName,
		})
	}
	return SearchResponse{
		Items:    out,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	}, nil
}

func openQueryDB(path string) (*sql.DB, error) {
	return dbx.Open(path)
}

func (s *Service) ListContracts(page int, pageSize int) (SearchResponse, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	if pageSize > 2000 {
		pageSize = 2000
	}

	if err := s.index.EnsureFresh(); err != nil {
		return SearchResponse{}, err
	}

	db, err := openQueryDB(s.dbPath)
	if err != nil {
		return SearchResponse{}, fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()

	var total int
	if err := db.QueryRow(`SELECT COUNT(1) FROM kline_search_index WHERE kind='contract'`).Scan(&total); err != nil {
		return SearchResponse{}, fmt.Errorf("count contract index failed: %w", err)
	}

	offset := (page - 1) * pageSize
	rows, err := db.Query(`
SELECT symbol,variety,exchange,min_time,max_time,bar_count,table_name
FROM kline_search_index
WHERE kind='contract'
ORDER BY max_time DESC, symbol ASC
LIMIT ? OFFSET ?`, pageSize, offset)
	if err != nil {
		return SearchResponse{}, fmt.Errorf("query contract index failed: %w", err)
	}
	defer rows.Close()

	items := make([]SearchItem, 0, pageSize)
	for rows.Next() {
		var (
			symbol   string
			variety  string
			exchange string
			minTime  time.Time
			maxTime  time.Time
			barCount int64
			table    string
		)
		if err := rows.Scan(&symbol, &variety, &exchange, &minTime, &maxTime, &barCount, &table); err != nil {
			return SearchResponse{}, fmt.Errorf("scan contract row failed: %w", err)
		}
		items = append(items, SearchItem{
			Type:      "contract",
			Symbol:    displaySymbol(symbol, "contract"),
			Variety:   variety,
			Exchange:  exchange,
			MinTime:   minTime.Format("2006-01-02 15:04"),
			MaxTime:   maxTime.Format("2006-01-02 15:04"),
			BarCount:  barCount,
			TableName: table,
		})
	}
	if err := rows.Err(); err != nil {
		return SearchResponse{}, fmt.Errorf("iterate contract rows failed: %w", err)
	}

	return SearchResponse{
		Items:    items,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	}, nil
}

func (s *Service) BarsByEnd(symbol string, kind string, variety string, end time.Time, limit int) (BarsResponse, error) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if kind != "contract" && kind != "l9" {
		return BarsResponse{}, fmt.Errorf("invalid type: %s", kind)
	}
	if limit <= 0 {
		limit = 2000
	}
	if limit > 5000 {
		limit = 5000
	}
	item, err := s.index.LookupBySymbol(symbol, kind, variety)
	if err != nil {
		return BarsResponse{}, err
	}
	if item == nil {
		logger.Info("kline query lookup miss", "symbol", symbol, "type", kind, "variety", variety)
		return BarsResponse{}, sql.ErrNoRows
	}

	symbolCandidates := instrumentIDCandidates(item.Symbol, item.Variety, kind)
	if len(symbolCandidates) == 0 {
		return BarsResponse{}, sql.ErrNoRows
	}
	primarySymbol := symbolCandidates[0]
	secondarySymbol := symbolCandidates[0]
	if len(symbolCandidates) > 1 {
		secondarySymbol = symbolCandidates[1]
	}

	db, err := openQueryDB(s.dbPath)
	if err != nil {
		return BarsResponse{}, fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()
	timeExpr, err := resolveQueryTimeExpr(db, item.TableName)
	if err != nil {
		return BarsResponse{}, err
	}
	adjustedExpr, err := resolveAdjustedTimeExpr(db, item.TableName, timeExpr)
	if err != nil {
		return BarsResponse{}, err
	}

	query := fmt.Sprintf(`
SELECT %s AS "qtime","Open","High","Low","Close","Volume","OpenInterest","Exchange",%s AS "__data_time",%s AS "__sort_time"
FROM "%s"
WHERE (
    lower("InstrumentID") = ?
 OR lower("InstrumentID") = ?
)
  AND "Period" = '1m'
  AND %s <= ?
ORDER BY "__sort_time" DESC
LIMIT ?`, adjustedExpr, timeExpr, adjustedExpr, item.TableName, adjustedExpr)

	loadRows := func(endTime time.Time) ([]KlineBar, string, error) {
		args := []any{
			primarySymbol,
			secondarySymbol,
			endTime.Format("2006-01-02 15:04:00"),
			limit,
		}
		logger.Info("kline query execute",
			"table", item.TableName,
			"args_symbol", primarySymbol,
			"args_symbol_fallback", secondarySymbol,
			"args_end", endTime.Format("2006-01-02 15:04:00"),
			"args_limit", limit,
			"sql", renderExecutableSQL(query, args...),
		)
		rows, qErr := db.Query(
			query, args...,
		)
		if qErr != nil {
			return nil, "", qErr
		}
		defer rows.Close()

		var bars []KlineBar
		exchange := item.Exchange
		for rows.Next() {
			var ts, dataTs, sortTime time.Time
			var ex string
			var bar KlineBar
			if err := rows.Scan(&ts, &bar.Open, &bar.High, &bar.Low, &bar.Close, &bar.Volume, &bar.OpenInterest, &ex, &dataTs, &sortTime); err != nil {
				return nil, "", err
			}
			bar.AdjustedTime = ts.Unix()
			if dataTs.IsZero() {
				bar.DataTime = bar.AdjustedTime
			} else {
				bar.DataTime = dataTs.Unix()
			}
			bars = append(bars, bar)
			if ex != "" {
				exchange = ex
			}
		}
		if err := rows.Err(); err != nil {
			return nil, "", err
		}
		if len(bars) > 0 {
			logger.Info("kline query result",
				"table", item.TableName,
				"rows", len(bars),
				"first_time", time.Unix(bars[len(bars)-1].AdjustedTime, 0).Format("2006-01-02 15:04:05"),
				"last_time", time.Unix(bars[0].AdjustedTime, 0).Format("2006-01-02 15:04:05"),
			)
		} else {
			logger.Info("kline query result empty", "table", item.TableName)
		}
		return bars, exchange, nil
	}

	bars, exchange, err := loadRows(end)
	if err != nil {
		return BarsResponse{}, fmt.Errorf("query bars failed: %w", err)
	}
	// 若按传入 end 查不到，回退到“当前时间”再取一段，避免前端时间传参造成空图
	if len(bars) == 0 {
		logger.Info("kline query fallback", "symbol", symbol, "type", kind, "fallback_end", time.Now().Format("2006-01-02 15:04:00"))
		fallbackBars, fallbackExchange, fbErr := loadRows(time.Now())
		if fbErr != nil {
			return BarsResponse{}, fmt.Errorf("fallback query bars failed: %w", fbErr)
		}
		bars = fallbackBars
		exchange = fallbackExchange
	}
	if len(bars) == 0 {
		return BarsResponse{}, sql.ErrNoRows
	}
	reverseBars(bars)

	var closes []float64
	closes = closes[:0]
	for _, bar := range bars {
		closes = append(closes, bar.Close)
	}

	dif, dea, hist := calcMACD(closes, 12, 26, 9)
	macd := make([]MACDPoint, len(bars))
	for i := range bars {
		macd[i] = MACDPoint{
			Time: bars[i].AdjustedTime,
			DIF:  dif[i],
			DEA:  dea[i],
			Hist: hist[i],
		}
	}

	return BarsResponse{
		Meta: BarsMeta{
			Symbol:   displaySymbol(item.Symbol, kind),
			Type:     kind,
			Variety:  item.Variety,
			Exchange: exchange,
		},
		Bars: bars,
		MACD: macd,
	}, nil
}

func tableHasColumn(db *sql.DB, tableName string, column string) (bool, error) {
	rows, err := db.Query(`
SELECT column_name
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = ?`, tableName)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}
	return false, rows.Err()
}

func resolveQueryTimeExpr(db *sql.DB, tableName string) (string, error) {
	dataTimeCol := ""
	hasDataTime, err := tableHasColumn(db, tableName, "DataTime")
	if err != nil {
		return "", fmt.Errorf("inspect table %s DataTime failed: %w", tableName, err)
	}
	if hasDataTime {
		dataTimeCol = `"DataTime"`
	} else {
		hasLegacyTime, err := tableHasColumn(db, tableName, "Time")
		if err != nil {
			return "", fmt.Errorf("inspect table %s Time failed: %w", tableName, err)
		}
		if !hasLegacyTime {
			return "", fmt.Errorf("table %s missing both DataTime and Time columns", tableName)
		}
		dataTimeCol = `"Time"`
	}
	return dataTimeCol, nil
}

func resolveAdjustedTimeExpr(db *sql.DB, tableName string, fallback string) (string, error) {
	hasAdjusted, err := tableHasColumn(db, tableName, "AdjustedTime")
	if err != nil {
		return "", fmt.Errorf("inspect table %s AdjustedTime failed: %w", tableName, err)
	}
	if hasAdjusted {
		return `"AdjustedTime"`, nil
	}
	return fallback, nil
}

func reverseBars(items []KlineBar) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

func calcMACD(closes []float64, shortPeriod, longPeriod, signalPeriod int) ([]*float64, []*float64, []*float64) {
	n := len(closes)
	dif := make([]*float64, n)
	dea := make([]*float64, n)
	hist := make([]*float64, n)
	if n == 0 {
		return dif, dea, hist
	}

	shortK := 2.0 / float64(shortPeriod+1)
	longK := 2.0 / float64(longPeriod+1)
	signalK := 2.0 / float64(signalPeriod+1)

	var shortEMA, longEMA, signalEMA float64
	for i, close := range closes {
		if i == 0 {
			shortEMA = close
			longEMA = close
			signalEMA = 0
		} else {
			shortEMA = close*shortK + shortEMA*(1-shortK)
			longEMA = close*longK + longEMA*(1-longK)
		}
		d := shortEMA - longEMA
		if i == 0 {
			signalEMA = d
		} else {
			signalEMA = d*signalK + signalEMA*(1-signalK)
		}
		h := (d - signalEMA) * 2

		dCopy := d
		sCopy := signalEMA
		hCopy := h
		dif[i] = &dCopy
		dea[i] = &sCopy
		hist[i] = &hCopy
	}
	return dif, dea, hist
}

func displaySymbol(symbol string, kind string) string {
	_ = kind
	return strings.ToLower(strings.TrimSpace(symbol))
}

func instrumentIDCandidates(symbol string, variety string, kind string) []string {
	s := strings.ToLower(strings.TrimSpace(symbol))
	v := strings.ToLower(strings.TrimSpace(variety))
	if kind == "l9" {
		out := make([]string, 0, 4)
		if s != "" {
			out = append(out, s)
			if !strings.HasSuffix(s, "l9") && v != "" {
				out = append(out, v+"l9")
			}
		}
		out = append(out, "l9")
		return dedupeStrings(out)
	}
	if s == "" {
		return nil
	}
	out := []string{s}
	if v != "" && strings.HasPrefix(s, v) {
		out = append(out, strings.TrimPrefix(s, v))
	}
	return dedupeStrings(out)
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
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

func renderExecutableSQL(query string, args ...any) string {
	out := strings.TrimSpace(query)
	for _, arg := range args {
		out = strings.Replace(out, "?", mysqlLiteral(arg), 1)
	}
	out = strings.Join(strings.Fields(out), " ")
	return strings.ReplaceAll(out, `"`, "`")
}

func mysqlLiteral(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(x), "'", "''") + "'"
	case bool:
		if x {
			return "1"
		}
		return "0"
	case int:
		return strconv.Itoa(x)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case time.Time:
		return "'" + x.Format("2006-01-02 15:04:05") + "'"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(v), "'", "''") + "'"
	}
}
