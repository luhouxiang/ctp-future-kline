package klinequery

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/mmkline"
	"ctp-future-kline/internal/searchindex"
)

type SearchItem struct {
	// Type 表示命中数据类型，例如 contract 或 l9。
	Type string `json:"type"`
	// Symbol 是展示给前端的合约或指数代码。
	Symbol string `json:"symbol"`
	// Variety 是品种代码。
	Variety string `json:"variety"`
	// Exchange 是交易所代码。
	Exchange string `json:"exchange"`
	// BarCount 是索引表中记录的 K 线数量。
	BarCount int64 `json:"bar_count"`
	// UpdatedAt 是索引最近一次更新时间。
	UpdatedAt string `json:"updated_at"`
	// TableName 是候选项对应的底层表名。
	TableName string `json:"table_name"`
}

type SearchResponse struct {
	// Items 是当前页结果列表。
	Items []SearchItem `json:"items"`
	// Total 是总命中数量。
	Total int `json:"total"`
	// Page 是当前页码。
	Page int `json:"page"`
	// PageSize 是当前页大小。
	PageSize int `json:"page_size"`
}

type KlineBar struct {
	AdjustedTime int64   `json:"adjusted_time"`
	DataTime     int64   `json:"data_time"`
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
	dbPath        string
	sessionDBPath string
	index         *searchindex.Manager
}

type searchTable struct {
	Name    string
	Kind    string
	Variety string
}

type symbolRow struct {
	Symbol   string
	Exchange string
}

func NewService(dbPath string, index *searchindex.Manager) *Service {
	return &Service{dbPath: dbPath, sessionDBPath: dbPath, index: index}
}

func NewServiceWithSessionDB(dbPath string, sessionDBPath string, index *searchindex.Manager) *Service {
	if strings.TrimSpace(sessionDBPath) == "" {
		sessionDBPath = dbPath
	}
	return &Service{dbPath: dbPath, sessionDBPath: sessionDBPath, index: index}
}

func (s *Service) Search(keyword string, page int, pageSize int) (SearchResponse, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	if pageSize > 200 {
		pageSize = 200
	}

	db, err := openQueryDB(s.dbPath)
	if err != nil {
		return SearchResponse{}, fmt.Errorf("open mysql failed: %w", err)
	}
	defer db.Close()

	tables, err := listSearchTables(db)
	if err != nil {
		return SearchResponse{}, err
	}
	items, err := s.searchCandidates(db, tables, keyword)
	if err != nil {
		return SearchResponse{}, err
	}

	total := len(items)
	startIdx, endIdx := pageSlice(total, page, pageSize)
	paged := make([]SearchItem, 0, endIdx-startIdx)
	if startIdx < endIdx {
		paged = append(paged, items[startIdx:endIdx]...)
	}
	if err := s.fillIndexMetadata(paged); err != nil {
		return SearchResponse{}, err
	}

	return SearchResponse{
		Items:    paged,
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
SELECT symbol,variety,exchange,bar_count,updated_at,table_name
FROM kline_search_index
WHERE kind='contract'
ORDER BY updated_at DESC, symbol ASC
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
			barCount int64
			updated  time.Time
			table    string
		)
		if err := rows.Scan(&symbol, &variety, &exchange, &barCount, &updated, &table); err != nil {
			return SearchResponse{}, fmt.Errorf("scan contract row failed: %w", err)
		}
		items = append(items, SearchItem{
			Type:      "contract",
			Symbol:    displaySymbol(symbol, "contract"),
			Variety:   variety,
			Exchange:  exchange,
			BarCount:  barCount,
			UpdatedAt: updated.Format("2006-01-02 15:04:05"),
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

func (s *Service) BarsByEnd(symbol string, kind string, variety string, timeframe string, end time.Time, limit int) (BarsResponse, error) {
	kind = normalizeKlineKind(kind, symbol)
	if kind != "contract" && kind != "l9" {
		return BarsResponse{}, fmt.Errorf("invalid type: %s", kind)
	}
	tf, _, err := normalizeTimeframe(timeframe)
	if err != nil {
		return BarsResponse{}, err
	}
	logger.Info("kline pipeline", "stage", "bars_begin", "symbol", symbol, "kind", kind, "variety", variety, "timeframe", tf, "limit", limit, "end", end.Format("2006-01-02 15:04:05"))
	if limit <= 0 {
		limit = 2000
	}
	if limit > 5000 {
		limit = 5000
	}

	item, err := s.index.RefreshBySymbol(symbol, kind, variety)
	if err != nil {
		if !errors.Is(err, searchindex.ErrBusy) {
			return BarsResponse{}, err
		}
		item, err = s.index.LookupBySymbol(symbol, kind, variety)
		if err != nil {
			return BarsResponse{}, err
		}
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

	queryTable := item.TableName
	queryPeriod := "1m"
	if tf != "1m" {
		sessionDB, sessOpenErr := openQueryDB(s.sessionDBPath)
		if sessOpenErr != nil {
			return BarsResponse{}, fmt.Errorf("open session mysql failed: %w", sessOpenErr)
		}
		defer sessionDB.Close()
		if _, sessErr := ensureCompletedTradingSession(sessionDB, item.Variety); sessErr != nil {
			return BarsResponse{}, sessErr
		}
		if kind == "l9" {
			queryTable, err = mmkline.TableNameForL9MMVariety(item.Variety)
		} else {
			queryTable, err = mmkline.TableNameForInstrumentMMVariety(item.Variety)
		}
		if err != nil {
			return BarsResponse{}, err
		}
		queryPeriod = tf
	}

	timeExpr := `"DataTime"`
	adjustedExpr := `"AdjustedTime"`

	query := fmt.Sprintf(`
SELECT %s AS "qtime","Open","High","Low","Close","Volume","OpenInterest","Exchange",%s AS "__data_time",%s AS "__sort_time"
FROM "%s"
WHERE (
    lower("InstrumentID") = ?
 OR lower("InstrumentID") = ?
)
  AND "Period" = ?
  AND %s >= ?
  AND %s <= ?
ORDER BY "__sort_time" DESC
LIMIT ?`, adjustedExpr, timeExpr, adjustedExpr, queryTable, adjustedExpr, adjustedExpr)

	loadRows := func(endTime time.Time) ([]KlineBar, string, error) {
		startTime := endTime.AddDate(0, -1, 0)
		args := []any{
			primarySymbol,
			secondarySymbol,
			queryPeriod,
			startTime.Format("2006-01-02 15:04:00"),
			endTime.Format("2006-01-02 15:04:00"),
			limit,
		}
		logger.Info("kline query execute",
			"table", queryTable,
			"period", queryPeriod,
			"args_symbol", primarySymbol,
			"args_symbol_fallback", secondarySymbol,
			"args_start", startTime.Format("2006-01-02 15:04:00"),
			"args_end", endTime.Format("2006-01-02 15:04:00"),
			"args_limit", limit,
			"sql", renderExecutableSQL(query, args...),
		)
		rows, qErr := db.Query(query, args...)
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
				"table", queryTable,
				"rows", len(bars),
				"first_time", time.Unix(bars[len(bars)-1].AdjustedTime, 0).Format("2006-01-02 15:04:05"),
				"last_time", time.Unix(bars[0].AdjustedTime, 0).Format("2006-01-02 15:04:05"),
			)
		} else {
			logger.Info("kline query result empty", "table", queryTable, "period", queryPeriod)
		}
		return bars, exchange, nil
	}

	bars, exchange, err := loadRows(end)
	if err != nil {
		return BarsResponse{}, fmt.Errorf("query bars failed: %w", err)
	}
	if len(bars) == 0 {
		return BarsResponse{}, sql.ErrNoRows
	}
	reverseBars(bars)
	logDuplicateAdjustedTimes(bars, symbol, kind, item.Variety, queryTable, queryPeriod)
	logger.Info("kline pipeline", "stage", "load_done", "symbol", symbol, "kind", kind, "variety", item.Variety, "timeframe", tf, "period", queryPeriod, "table", queryTable, "rows", len(bars), "first_time", time.Unix(bars[0].AdjustedTime, 0).Format("2006-01-02 15:04:05"), "last_time", time.Unix(bars[len(bars)-1].AdjustedTime, 0).Format("2006-01-02 15:04:05"))

	var closes []float64
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
	logger.Info("kline pipeline", "stage", "bars_done", "symbol", symbol, "kind", kind, "variety", item.Variety, "timeframe", tf, "bar_count", len(bars))

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

func (s *Service) RebuildAllIndex() error {
	return s.index.RebuildAll()
}

func (s *Service) RebuildSearchItems(items []SearchItem) error {
	targets := make([]searchindex.Target, 0, len(items))
	for _, item := range items {
		targets = append(targets, searchindex.Target{
			TableName: item.TableName,
			Symbol:    item.Symbol,
			Variety:   item.Variety,
			Kind:      item.Type,
		})
	}
	return s.index.RebuildItems(targets)
}

func (s *Service) RebuildSearchItem(item SearchItem) (SearchItem, bool, error) {
	if err := s.RebuildSearchItems([]SearchItem{item}); err != nil {
		return SearchItem{}, false, err
	}
	meta, err := s.index.LookupBySymbol(item.Symbol, item.Type, item.Variety)
	if err != nil {
		return SearchItem{}, false, err
	}
	if meta == nil {
		return SearchItem{
			Type:      item.Type,
			Symbol:    item.Symbol,
			Variety:   item.Variety,
			Exchange:  item.Exchange,
			TableName: item.TableName,
		}, false, nil
	}
	return SearchItem{
		Type:      meta.Kind,
		Symbol:    displaySymbol(meta.Symbol, meta.Kind),
		Variety:   meta.Variety,
		Exchange:  meta.Exchange,
		BarCount:  meta.BarCount,
		UpdatedAt: meta.UpdatedAt.Format("2006-01-02 15:04:05"),
		TableName: meta.TableName,
	}, true, nil
}

func (s *Service) searchCandidates(db *sql.DB, tables []searchTable, keyword string) ([]SearchItem, error) {
	keyword = normalizeSearchKeyword(keyword)
	if keyword == "" {
		items, err := s.defaultSearchItemsFromIndex(db, 10)
		if err != nil {
			return nil, err
		}
		if len(items) > 0 {
			return items, nil
		}
		return s.defaultSearchItemsFromTables(db, tables, 10)
	}

	items, err := s.searchItemsFromIndex(db, keyword)
	if err != nil {
		return nil, err
	}
	if len(items) > 0 {
		return items, nil
	}
	return s.searchCandidatesFromTables(db, tables, keyword)
}

func (s *Service) searchItemsFromIndex(db *sql.DB, keyword string) ([]SearchItem, error) {
	if keyword == "" {
		return nil, nil
	}
	if ok, err := searchIndexTableExists(db); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}

	prefixLike := keyword + "%"
	containsLike := "%" + keyword + "%"
	rows, err := db.Query(`
SELECT symbol,variety,exchange,kind,bar_count,updated_at,table_name,
       CASE
         WHEN symbol_norm LIKE ? THEN 0
         WHEN symbol_norm LIKE ? THEN 1
         WHEN variety LIKE ? THEN 2
         WHEN variety LIKE ? THEN 3
         WHEN lower(exchange) LIKE ? THEN 4
         WHEN lower(exchange) LIKE ? THEN 5
         ELSE 6
       END AS match_rank,
       CASE WHEN kind='l9' THEN 0 ELSE 1 END AS kind_rank
FROM kline_search_index
WHERE symbol_norm LIKE ?
   OR symbol_norm LIKE ?
   OR variety LIKE ?
   OR variety LIKE ?
   OR lower(exchange) LIKE ?
   OR lower(exchange) LIKE ?
ORDER BY match_rank ASC, kind_rank ASC, updated_at DESC, symbol_norm ASC`,
		prefixLike,
		containsLike,
		prefixLike,
		containsLike,
		prefixLike,
		containsLike,
		prefixLike,
		containsLike,
		prefixLike,
		containsLike,
		prefixLike,
		containsLike,
	)
	if err != nil {
		return nil, fmt.Errorf("query search index candidates failed: %w", err)
	}
	defer rows.Close()

	items := make([]SearchItem, 0, 128)
	for rows.Next() {
		var (
			item      SearchItem
			updatedAt time.Time
			matchRank int
			kindRank  int
		)
		if err := rows.Scan(
			&item.Symbol,
			&item.Variety,
			&item.Exchange,
			&item.Type,
			&item.BarCount,
			&updatedAt,
			&item.TableName,
			&matchRank,
			&kindRank,
		); err != nil {
			return nil, fmt.Errorf("scan search index candidate failed: %w", err)
		}
		item.Symbol = displaySymbol(item.Symbol, item.Type)
		item.UpdatedAt = updatedAt.Format("2006-01-02 15:04:05")
		item.Exchange = strings.ToUpper(strings.TrimSpace(item.Exchange))
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate search index candidates failed: %w", err)
	}
	return dedupeSearchItems(items), nil
}

func (s *Service) defaultSearchItemsFromIndex(db *sql.DB, limit int) ([]SearchItem, error) {
	if limit <= 0 {
		return nil, nil
	}
	if ok, err := searchIndexTableExists(db); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}

	items := make([]SearchItem, 0, limit)
	appendByKind := func(kind string) error {
		if len(items) >= limit {
			return nil
		}
		rows, err := db.Query(`
SELECT symbol,variety,exchange,kind,bar_count,updated_at,table_name
FROM kline_search_index
WHERE kind = ?
ORDER BY updated_at DESC, symbol_norm ASC
LIMIT ?`, kind, limit-len(items))
		if err != nil {
			return fmt.Errorf("query default search index items failed: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var (
				item      SearchItem
				updatedAt time.Time
			)
			if err := rows.Scan(
				&item.Symbol,
				&item.Variety,
				&item.Exchange,
				&item.Type,
				&item.BarCount,
				&updatedAt,
				&item.TableName,
			); err != nil {
				return fmt.Errorf("scan default search index item failed: %w", err)
			}
			item.Symbol = displaySymbol(item.Symbol, item.Type)
			item.UpdatedAt = updatedAt.Format("2006-01-02 15:04:05")
			item.Exchange = strings.ToUpper(strings.TrimSpace(item.Exchange))
			items = append(items, item)
		}
		return rows.Err()
	}

	if err := appendByKind("l9"); err != nil {
		return nil, err
	}
	if err := appendByKind("contract"); err != nil {
		return nil, err
	}
	return dedupeSearchItems(items), nil
}

func searchIndexTableExists(db *sql.DB) (bool, error) {
	var count int
	if err := db.QueryRow(`
SELECT COUNT(1)
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name = 'kline_search_index'`).Scan(&count); err != nil {
		return false, fmt.Errorf("query search index table existence failed: %w", err)
	}
	return count > 0, nil
}

func (s *Service) searchCandidatesFromTables(db *sql.DB, tables []searchTable, keyword string) ([]SearchItem, error) {
	letters, digits := splitKeyword(strings.ToLower(strings.TrimSpace(keyword)))
	if letters == "" {
		return nil, nil
	}

	l9Tables := make(map[string]searchTable)
	contractTables := make([]searchTable, 0, len(tables))
	for _, table := range tables {
		if table.Variety == "" || !strings.HasPrefix(table.Variety, letters) {
			continue
		}
		if table.Kind == "l9" {
			l9Tables[table.Variety] = table
		}
		if table.Kind == "contract" {
			contractTables = append(contractTables, table)
		}
	}

	results := make([]SearchItem, 0, 64)
	includedL9 := make(map[string]struct{})
	for _, table := range contractTables {
		rows, err := fetchContractSymbols(db, table.Name, digits)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			results = append(results, SearchItem{
				Type:      "contract",
				Symbol:    displaySymbol(row.Symbol, "contract"),
				Variety:   table.Variety,
				Exchange:  row.Exchange,
				TableName: table.Name,
			})
			if l9Table, ok := l9Tables[table.Variety]; ok {
				if _, exists := includedL9[table.Variety]; !exists {
					item, fetchErr := fetchFirstSymbol(db, l9Table.Name, "l9", l9Table.Variety)
					if fetchErr != nil {
						return nil, fetchErr
					}
					if item != nil {
						results = append(results, *item)
						includedL9[table.Variety] = struct{}{}
					}
				}
			}
		}
	}

	if digits == "" {
		for variety, table := range l9Tables {
			if _, exists := includedL9[variety]; exists {
				continue
			}
			item, err := fetchFirstSymbol(db, table.Name, "l9", variety)
			if err != nil {
				return nil, err
			}
			if item != nil {
				results = append(results, *item)
			}
		}
	}

	return dedupeSearchItems(results), nil
}

func (s *Service) defaultSearchItemsFromTables(db *sql.DB, tables []searchTable, limit int) ([]SearchItem, error) {
	results := make([]SearchItem, 0, limit)
	for _, table := range tables {
		if len(results) >= limit {
			break
		}
		if table.Kind != "l9" {
			continue
		}
		item, err := fetchFirstSymbol(db, table.Name, "l9", table.Variety)
		if err != nil {
			return nil, err
		}
		if item != nil {
			results = append(results, *item)
		}
	}
	if len(results) < limit {
		for _, table := range tables {
			if len(results) >= limit {
				break
			}
			if table.Kind != "contract" {
				continue
			}
			item, err := fetchFirstSymbol(db, table.Name, "contract", table.Variety)
			if err != nil {
				return nil, err
			}
			if item != nil {
				results = append(results, *item)
			}
		}
	}
	return dedupeSearchItems(results), nil
}

func (s *Service) fillIndexMetadata(items []SearchItem) error {
	if len(items) == 0 {
		return nil
	}
	targets := make([]searchindex.Target, 0, len(items))
	for _, item := range items {
		targets = append(targets, searchindex.Target{
			TableName: item.TableName,
			Symbol:    item.Symbol,
			Variety:   item.Variety,
			Kind:      item.Type,
		})
	}
	lookup, err := s.index.LookupItems(targets)
	if err != nil {
		return err
	}
	for idx := range items {
		key := searchIndexTargetKey(items[idx])
		meta, ok := lookup[key]
		if !ok {
			continue
		}
		items[idx].BarCount = meta.BarCount
		items[idx].UpdatedAt = meta.UpdatedAt.Format("2006-01-02 15:04:05")
		if items[idx].Exchange == "" {
			items[idx].Exchange = meta.Exchange
		}
		if items[idx].TableName == "" {
			items[idx].TableName = meta.TableName
		}
	}
	return nil
}

func listSearchTables(db *sql.DB) ([]searchTable, error) {
	rows, err := db.Query(`
SELECT table_name
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND (table_name LIKE 'future_kline_l9_1m_%' OR table_name LIKE 'future_kline_instrument_1m_%')
ORDER BY table_name`)
	if err != nil {
		return nil, fmt.Errorf("list search tables failed: %w", err)
	}
	defer rows.Close()

	out := make([]searchTable, 0, 256)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scan search table name failed: %w", err)
		}
		kind := tableKindFromTableName(tableName)
		variety := varietyFromTableName(tableName)
		if kind == "" || variety == "" {
			continue
		}
		out = append(out, searchTable{Name: tableName, Kind: kind, Variety: variety})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate search tables failed: %w", err)
	}
	return out, nil
}

func tableKindFromTableName(tableName string) string {
	switch {
	case strings.HasPrefix(tableName, "future_kline_l9_1m_"):
		return "l9"
	case strings.HasPrefix(tableName, "future_kline_instrument_1m_"):
		return "contract"
	default:
		return ""
	}
}

func varietyFromTableName(tableName string) string {
	switch {
	case strings.HasPrefix(tableName, "future_kline_l9_1m_"):
		return normalizeSearchVariety(strings.TrimPrefix(tableName, "future_kline_l9_1m_"))
	case strings.HasPrefix(tableName, "future_kline_instrument_1m_"):
		return normalizeSearchVariety(strings.TrimPrefix(tableName, "future_kline_instrument_1m_"))
	default:
		return ""
	}
}

func normalizeSearchVariety(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	end := 0
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return ""
	}
	return value[:end]
}

func fetchFirstSymbol(db *sql.DB, tableName string, kind string, variety string) (*SearchItem, error) {
	rows, err := db.Query(`
SELECT lower("InstrumentID"), MAX("Exchange")
FROM "` + tableName + `"
GROUP BY lower("InstrumentID")
ORDER BY lower("InstrumentID")
LIMIT 1`)
	if err != nil {
		return nil, fmt.Errorf("query first symbol from %s failed: %w", tableName, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	var symbol, exchange string
	if err := rows.Scan(&symbol, &exchange); err != nil {
		return nil, fmt.Errorf("scan first symbol from %s failed: %w", tableName, err)
	}
	return &SearchItem{
		Type:      kind,
		Symbol:    displaySymbol(symbol, kind),
		Variety:   variety,
		Exchange:  strings.ToUpper(strings.TrimSpace(exchange)),
		TableName: tableName,
	}, nil
}

func fetchContractSymbols(db *sql.DB, tableName string, digits string) ([]symbolRow, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if digits == "" {
		rows, err = db.Query(`
SELECT lower("InstrumentID"), MAX("Exchange")
FROM "` + tableName + `"
GROUP BY lower("InstrumentID")
ORDER BY lower("InstrumentID")`)
	} else {
		rows, err = db.Query(`
SELECT lower("InstrumentID"), MAX("Exchange")
FROM "`+tableName+`"
WHERE lower("InstrumentID") LIKE ?
GROUP BY lower("InstrumentID")
ORDER BY lower("InstrumentID")`, "%"+digits+"%")
	}
	if err != nil {
		return nil, fmt.Errorf("query contract symbols from %s failed: %w", tableName, err)
	}
	defer rows.Close()

	out := make([]symbolRow, 0, 64)
	for rows.Next() {
		var row symbolRow
		if err := rows.Scan(&row.Symbol, &row.Exchange); err != nil {
			return nil, fmt.Errorf("scan contract symbol from %s failed: %w", tableName, err)
		}
		row.Exchange = strings.ToUpper(strings.TrimSpace(row.Exchange))
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate contract symbols from %s failed: %w", tableName, err)
	}
	return out, nil
}

func splitKeyword(keyword string) (string, string) {
	keyword = normalizeSearchKeyword(keyword)
	if keyword == "" {
		return "", ""
	}
	var letters strings.Builder
	var digits strings.Builder
	for _, ch := range keyword {
		switch {
		case ch >= 'a' && ch <= 'z':
			if digits.Len() == 0 {
				letters.WriteRune(ch)
			}
		case ch >= '0' && ch <= '9':
			digits.WriteRune(ch)
		}
	}
	return letters.String(), digits.String()
}

func normalizeSearchKeyword(keyword string) string {
	keyword = strings.ToLower(strings.TrimSpace(keyword))
	if keyword == "" {
		return ""
	}
	var out strings.Builder
	out.Grow(len(keyword))
	for _, ch := range keyword {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') {
			out.WriteRune(ch)
		}
	}
	return out.String()
}

func dedupeSearchItems(items []SearchItem) []SearchItem {
	seen := make(map[string]struct{}, len(items))
	out := make([]SearchItem, 0, len(items))
	for _, item := range items {
		key := item.Type + "|" + strings.ToLower(strings.TrimSpace(item.Symbol))
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func pageSlice(total int, page int, pageSize int) (int, int) {
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return start, end
}

func searchIndexTargetKey(item SearchItem) string {
	return strings.ToLower(strings.TrimSpace(item.Symbol)) + "|" + strings.ToLower(strings.TrimSpace(item.Type)) + "|" + normalizeSearchVariety(item.Variety)
}

func reverseBars(items []KlineBar) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

func logDuplicateAdjustedTimes(bars []KlineBar, symbol, kind, variety, table, period string) {
	if len(bars) < 2 {
		return
	}
	dupCount := 0
	samples := make([]string, 0, 5)
	for i := 1; i < len(bars); i++ {
		if bars[i].AdjustedTime != bars[i-1].AdjustedTime {
			continue
		}
		dupCount++
		if len(samples) < 5 {
			samples = append(samples, time.Unix(bars[i].AdjustedTime, 0).Format("2006-01-02 15:04:05"))
		}
	}
	if dupCount == 0 {
		return
	}
	logger.Error("kline duplicate adjusted_time detected",
		"symbol", symbol,
		"kind", kind,
		"variety", variety,
		"table", table,
		"period", period,
		"duplicate_pairs", dupCount,
		"sample_times", strings.Join(samples, ","),
	)
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

func normalizeKlineKind(kind string, symbol string) string {
	k := strings.ToLower(strings.TrimSpace(kind))
	if k != "" {
		return k
	}
	s := strings.ToLower(strings.TrimSpace(symbol))
	if s == "l9" || strings.HasSuffix(s, "l9") {
		return "l9"
	}
	return "contract"
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
