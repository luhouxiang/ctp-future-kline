package web

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ctp-future-kline/internal/appmode"
	dbx "ctp-future-kline/internal/db"
)

type startupCheckItem struct {
	Key       string `json:"key"`
	Title     string `json:"title"`
	Level     string `json:"level"`
	Status    string `json:"status"`
	Detail    string `json:"detail"`
	Action    string `json:"action,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type startupChecksResponse struct {
	Checks         []startupCheckItem `json:"checks"`
	TradingDay     string             `json:"trading_day"`
	GeneratedAt    string             `json:"generated_at"`
	NeedsAttention int                `json:"needs_attention"`
}

func (s *Server) handleStartupChecks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := s.startupChecks()
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) startupChecks() startupChecksResponse {
	tradingDay := ""
	if s.status != nil {
		tradingDay = strings.TrimSpace(s.status.TradingDay())
	}
	checks := make([]startupCheckItem, 0, 10)
	checks = append(checks, s.calendarStartupCheck())

	sharedDB, err := dbx.Open(s.sharedDSN)
	if err != nil {
		checks = append(checks, startupCheckItem{
			Key:    "shared_meta",
			Title:  "共享元数据库",
			Level:  "error",
			Status: "不可用",
			Detail: "无法打开共享元数据库: " + err.Error(),
		})
	} else {
		defer sharedDB.Close()
		metaChecks, effectiveDay := s.sharedMetaStartupChecks(sharedDB, tradingDay)
		if tradingDay == "" {
			tradingDay = effectiveDay
		}
		checks = append(checks, metaChecks...)
	}
	checks = append(checks, s.klineIndexStartupCheck())
	checks = append(checks, s.strategyStartupCheck())
	checks = append(checks, s.tradeStartupCheck())

	needs := 0
	for _, item := range checks {
		switch item.Level {
		case "warn", "error":
			needs++
		}
	}
	return startupChecksResponse{
		Checks:         checks,
		TradingDay:     tradingDay,
		GeneratedAt:    time.Now().Format(time.RFC3339),
		NeedsAttention: needs,
	}
}

func (s *Server) calendarStartupCheck() startupCheckItem {
	if s.calendar == nil {
		return startupCheckItem{
			Key:    "calendar",
			Title:  "交易日历",
			Level:  "error",
			Status: "不可用",
			Detail: "交易日历管理器未初始化。",
		}
	}
	st, err := s.calendar.Status(s.cfg.Calendar.MinFutureOpenDays)
	if err != nil {
		return startupCheckItem{
			Key:    "calendar",
			Title:  "交易日历",
			Level:  "error",
			Status: "读取失败",
			Detail: err.Error(),
			Action: "calendar_refresh",
		}
	}
	level := "ok"
	status := "已覆盖"
	action := ""
	if st.NeedsAutoUpdate {
		level = "warn"
		status = "需要补齐"
		action = "calendar_refresh"
	}
	return startupCheckItem{
		Key:       "calendar",
		Title:     "交易日历",
		Level:     level,
		Status:    status,
		Detail:    "开市日覆盖到 " + emptyDash(st.MaxOpenTradeDate) + "，距离覆盖上限 " + intText(st.DaysToHorizon) + " 天，要求至少 " + intText(st.NeedFutureDays) + " 天。",
		Action:    action,
		UpdatedAt: firstNonEmptyText(st.LastSuccessAt, st.LastCheckAt),
	}
}

func (s *Server) sharedMetaStartupChecks(db *sql.DB, runtimeTradingDay string) ([]startupCheckItem, string) {
	latestDay, instrumentCount, instrumentUpdatedAt, _ := latestInstrumentSync(db)
	effectiveDay := firstNonEmptyText(runtimeTradingDay, latestDay)
	out := make([]startupCheckItem, 0, 5)

	level := "ok"
	status := "已同步"
	action := ""
	detail := "最近同步交易日 " + emptyDash(latestDay) + "，合约数 " + intText(instrumentCount) + "。"
	if latestDay == "" || instrumentCount == 0 {
		level = "warn"
		status = "缺少合约目录"
		action = "start_runtime"
		detail = "未找到 CTP 合约目录。启动实时行情会先同步合约目录，否则订阅目标、交易所推断可能不准。"
	} else if runtimeTradingDay != "" && latestDay != runtimeTradingDay {
		level = "warn"
		status = "不是当前交易日"
		action = "start_runtime"
		detail = "运行态交易日 " + runtimeTradingDay + "，最近合约目录交易日 " + latestDay + "。"
	}
	out = append(out, startupCheckItem{
		Key:       "instrument_catalog",
		Title:     "当日合约目录",
		Level:     level,
		Status:    status,
		Detail:    detail,
		Action:    action,
		UpdatedAt: instrumentUpdatedAt,
	})

	sessionTotal, sessionCompleted, sessionUpdatedAt, sessionErr := tradingSessionSummary(db)
	if sessionErr != nil {
		out = append(out, startupCheckItem{Key: "trading_sessions", Title: "交易时段", Level: "error", Status: "读取失败", Detail: sessionErr.Error(), Action: "trading_sessions_import"})
	} else {
		level = "ok"
		status = "已配置"
		action = ""
		if sessionCompleted == 0 {
			level = "warn"
			status = "缺少完整时段"
			action = "trading_sessions_import"
		}
		out = append(out, startupCheckItem{
			Key:       "trading_sessions",
			Title:     "交易时段",
			Level:     level,
			Status:    status,
			Detail:    "完整品种 " + intText(sessionCompleted) + " / 总品种 " + intText(sessionTotal) + "。交易时段缺失会影响跨夜、K线聚合和高周期生成。",
			Action:    action,
			UpdatedAt: sessionUpdatedAt,
		})
	}

	productCount, productUpdatedAt, productErr := productExchangeSummary(db)
	if productErr != nil {
		out = append(out, startupCheckItem{Key: "product_exchange", Title: "品种交易所缓存", Level: "error", Status: "读取失败", Detail: productErr.Error(), Action: "start_runtime"})
	} else {
		level = "ok"
		status = "已加载"
		action = ""
		if productCount == 0 {
			level = "warn"
			status = "缺少缓存"
			action = "start_runtime"
		}
		out = append(out, startupCheckItem{
			Key:       "product_exchange",
			Title:     "品种交易所缓存",
			Level:     level,
			Status:    status,
			Detail:    "品种映射数 " + intText(productCount) + "。缺失时下单交易所推断和品种识别可能不准。",
			Action:    action,
			UpdatedAt: productUpdatedAt,
		})
	}

	commissionCount, commissionUpdatedAt, commissionErr := countBySyncTradingDay(db, "ctp_commission_rates", effectiveDay)
	out = append(out, rateCheck("commission_rates", "手续费率", commissionCount, instrumentCount, commissionUpdatedAt, commissionErr, effectiveDay, "缺失时实时模拟交易成本估算不准。"))

	marginCount, marginUpdatedAt, marginErr := countBySyncTradingDay(db, "ctp_margin_rates", effectiveDay)
	out = append(out, rateCheck("margin_rates", "保证金率", marginCount, instrumentCount, marginUpdatedAt, marginErr, effectiveDay, "缺失时实时模拟保证金和可用资金估算不准。"))

	return out, effectiveDay
}

func (s *Server) klineIndexStartupCheck() startupCheckItem {
	db, err := dbx.Open(s.realtimeDSN)
	if err != nil {
		return startupCheckItem{Key: "kline_index", Title: "K线搜索索引", Level: "error", Status: "读取失败", Detail: err.Error(), Action: "rebuild_all_index"}
	}
	defer db.Close()
	count, updatedAt, err := countAndMaxUpdatedAt(db, "kline_search_index", "")
	if err != nil {
		return startupCheckItem{Key: "kline_index", Title: "K线搜索索引", Level: "error", Status: "读取失败", Detail: err.Error(), Action: "rebuild_all_index"}
	}
	level := "ok"
	status := "可用"
	action := ""
	if count == 0 {
		level = "warn"
		status = "空索引"
		action = "rebuild_all_index"
	}
	return startupCheckItem{
		Key:       "kline_index",
		Title:     "K线搜索索引",
		Level:     level,
		Status:    status,
		Detail:    "索引条目 " + intText(count) + "。空索引会导致搜索不到已导入数据。",
		Action:    action,
		UpdatedAt: updatedAt,
	}
}

func (s *Server) strategyStartupCheck() startupCheckItem {
	if s.strategy == nil {
		return startupCheckItem{Key: "strategy", Title: "策略服务", Level: "info", Status: "未启用", Detail: "策略配置未启用，不影响行情和手动交易。"}
	}
	st := s.strategy.Status()
	level := "ok"
	status := "已连接"
	if !st.Connected {
		level = "warn"
		status = "未连接"
	}
	return startupCheckItem{
		Key:       "strategy",
		Title:     "策略服务",
		Level:     level,
		Status:    status,
		Detail:    "gRPC " + emptyDash(st.GRPCAddr) + "，定义 " + intText(st.Definitions) + "，实例 " + intText(st.Instances) + "。未连接时策略、回测和信号不可用。",
		UpdatedAt: timeText(st.LastHealthAt),
	}
}

func (s *Server) tradeStartupCheck() startupCheckItem {
	st := s.tradeStatusSnapshot()
	mode := s.currentAppMode()
	if appmode.Normalize(mode) == appmode.ReplayPaper {
		return startupCheckItem{Key: "trade", Title: "交易连接", Level: "info", Status: "回放模拟", Detail: "回放模拟不需要真实交易前置。"}
	}
	level := "ok"
	status := "可用"
	action := "trade_refresh"
	if !st.Enabled {
		level = "info"
		status = "未启用"
		action = ""
	} else if appmode.Normalize(mode) == appmode.LiveReal && (!st.TraderFront || !st.TraderLogin || !st.SettlementConfirmed) {
		level = "warn"
		status = "未就绪"
	}
	return startupCheckItem{
		Key:       "trade",
		Title:     "交易连接",
		Level:     level,
		Status:    status,
		Detail:    "账户 " + emptyDash(st.AccountID) + "，交易日 " + emptyDash(st.TradingDay) + "。未登录或未结算确认时账户、持仓、下单不可用。",
		Action:    action,
		UpdatedAt: timeText(st.LastQueryAt),
	}
}

func latestInstrumentSync(db *sql.DB) (string, int, string, error) {
	var day string
	var count int
	var updated sql.NullTime
	err := db.QueryRow(`SELECT trading_day,instrument_count,updated_at FROM ctp_instrument_sync_log ORDER BY updated_at DESC LIMIT 1`).Scan(&day, &count, &updated)
	if err == sql.ErrNoRows {
		return "", 0, "", nil
	}
	if err != nil {
		return "", 0, "", err
	}
	return strings.TrimSpace(day), count, timeText(updated.Time), nil
}

func tradingSessionSummary(db *sql.DB) (int, int, string, error) {
	var total, completed int
	var updated sql.NullTime
	err := db.QueryRow(`SELECT COUNT(1), COALESCE(SUM(CASE WHEN is_completed=1 THEN 1 ELSE 0 END),0), MAX(updated_at) FROM trading_sessions`).Scan(&total, &completed, &updated)
	return total, completed, timeText(updated.Time), err
}

func productExchangeSummary(db *sql.DB) (int, string, error) {
	var count int
	var updated sql.NullTime
	err := db.QueryRow(`SELECT COUNT(1), MAX(updated_at) FROM ctp_product_exchange`).Scan(&count, &updated)
	return count, timeText(updated.Time), err
}

func countBySyncTradingDay(db *sql.DB, table string, tradingDay string) (int, string, error) {
	if strings.TrimSpace(tradingDay) == "" {
		return 0, "", nil
	}
	return countAndMaxUpdatedAt(db, table, strings.TrimSpace(tradingDay))
}

func countAndMaxUpdatedAt(db *sql.DB, table string, tradingDay string) (int, string, error) {
	var count int
	var updated sql.NullTime
	query := "SELECT COUNT(1), MAX(updated_at) FROM " + table
	var err error
	if tradingDay == "" {
		err = db.QueryRow(query).Scan(&count, &updated)
	} else {
		err = db.QueryRow(query+" WHERE sync_trading_day=?", tradingDay).Scan(&count, &updated)
	}
	return count, timeText(updated.Time), err
}

func rateCheck(key string, title string, count int, instrumentCount int, updatedAt string, err error, tradingDay string, impact string) startupCheckItem {
	if err != nil {
		return startupCheckItem{Key: key, Title: title, Level: "error", Status: "读取失败", Detail: err.Error(), Action: "trade_refresh"}
	}
	level := "ok"
	status := "已补齐"
	action := ""
	if strings.TrimSpace(tradingDay) == "" {
		level = "warn"
		status = "无法判断"
		action = "start_runtime"
	} else if count == 0 {
		level = "warn"
		status = "缺少数据"
		action = "trade_refresh"
	} else if instrumentCount > 0 && count < instrumentCount {
		level = "warn"
		status = "部分缺失"
		action = "trade_refresh"
	}
	return startupCheckItem{
		Key:       key,
		Title:     title,
		Level:     level,
		Status:    status,
		Detail:    "交易日 " + emptyDash(tradingDay) + "，已有 " + intText(count) + " / 合约目录 " + intText(instrumentCount) + "。" + impact,
		Action:    action,
		UpdatedAt: updatedAt,
	}
}

func emptyDash(v string) string {
	if strings.TrimSpace(v) == "" {
		return "--"
	}
	return strings.TrimSpace(v)
}

func firstNonEmptyText(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func intText(v int) string {
	return strconv.Itoa(v)
}

func timeText(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02 15:04:05")
}
