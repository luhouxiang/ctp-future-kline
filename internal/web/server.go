// server.go 是系统的 Web 控制面。
// 它负责组装行情运行时、回放、策略、实盘交易、查询等模块，并通过 HTTP 和 WebSocket
// 向前端页面暴露状态、控制接口和实时事件。
package web

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/appmode"
	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/calendar"
	"ctp-future-kline/internal/chartlayout"
	"ctp-future-kline/internal/config"
	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/importer"
	"ctp-future-kline/internal/klinequery"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/quotes"
	"ctp-future-kline/internal/replay"
	"ctp-future-kline/internal/searchindex"
	"ctp-future-kline/internal/strategy"
	"ctp-future-kline/internal/trade"
	"ctp-future-kline/internal/userconfig"

	"github.com/gorilla/websocket"
)

type Server struct {
	// cfg 保存整个应用配置，供路由和子模块装配时使用。
	cfg config.AppConfig
	// status 是行情运行时统一状态中心，对应 /api/status 中的 status 字段。
	status *quotes.RuntimeStatusCenter
	// runtime 是行情主链路启动入口，通常为 quotes.RuntimeManager。
	runtime runtimeStarter
	// searchRealtime/searchReplay 是不同数据域的索引管理器。
	searchRealtime *searchindex.Manager
	searchReplay   *searchindex.Manager
	// queryRealtime/queryReplay 提供不同数据域的 K 线查询接口。
	queryRealtime *klinequery.Service
	queryReplay   *klinequery.Service
	// calendar 管理交易日历查询与导入。
	calendar *calendar.Manager
	// replay 是回放任务调度服务，可为空表示未启用。
	replay *replay.Service
	// chartRealtime/chartReplay 是不同数据域的图表布局服务。
	chartRealtime *chartlayout.Service
	chartReplay   *chartlayout.Service
	// strategy 是策略子系统控制器，可为空表示未启用。
	strategy *strategy.Manager
	// tradeLive/tradePaperLive/tradePaperReplay 分别对应三种交易后端。
	tradeLive        *trade.Service
	tradePaperLive   *trade.Service
	tradePaperReplay *trade.Service
	// dsn 是兼容保留的默认 DSN。
	dsn string
	// 各逻辑库 DSN。
	sharedDSN           string
	realtimeDSN         string
	replayDSN           string
	tradeLiveDSN        string
	tradePaperLiveDSN   string
	tradePaperReplayDSN string
	// userConfig 保存用户级配置覆盖项。
	userConfig *userconfig.Store
	// currentMode 是服务端权威的全局运行模式。
	currentMode string

	// mu 保护导入会话和 websocket 连接集合等共享状态。
	mu sync.Mutex
	// wsWriteMu 串行化 websocket 写操作，避免多个 goroutine 同时写同一连接。
	wsWriteMu sync.Mutex
	// sessions 保存当前上传/导入任务会话。
	sessions map[string]*importer.TDXImportSession
	// wsConns 保存当前在线的 websocket 客户端连接。
	wsConns map[*websocket.Conn]*wsClient
	// chartStream 负责把实时/replay K 线事件分发给图表 websocket。
	chartStream *quotes.ChartStream
	replaySink  *quotes.ReplaySink
}

type wsClient struct {
	subs      map[string]quotes.ChartSubscription
	quoteSubs map[string]quotes.ChartSubscription
}

type runtimeStarter interface {
	Start() error
}

func NewServer(cfg config.AppConfig) *Server {
	status := quotes.NewRuntimeStatusCenter(time.Duration(cfg.Web.MarketOpenStaleSeconds) * time.Second)
	status.ConfigureQueueMonitoring(cfg.CTP)
	sharedDSN := dbx.DSNForRole(cfg.DB, dbx.RoleSharedMeta)
	realtimeDSN := dbx.DSNForRole(cfg.DB, dbx.RoleMarketRealtime)
	replayDSN := dbx.DSNForRole(cfg.DB, dbx.RoleMarketReplay)
	tradeLiveDSN := dbx.DSNForRole(cfg.DB, dbx.RoleTradeLive)
	tradePaperLiveDSN := dbx.DSNForRole(cfg.DB, dbx.RoleTradePaperLive)
	tradePaperReplayDSN := dbx.DSNForRole(cfg.DB, dbx.RoleTradePaperReplay)
	cfg.CTP.DBDSN = realtimeDSN
	cfg.CTP.SharedMetaDSN = sharedDSN
	searchRealtime := searchindex.NewManager(realtimeDSN, 30*time.Second)
	searchReplay := searchindex.NewManager(replayDSN, 30*time.Second)
	s := &Server{
		cfg:                 cfg,
		dsn:                 realtimeDSN,
		sharedDSN:           sharedDSN,
		realtimeDSN:         realtimeDSN,
		replayDSN:           replayDSN,
		tradeLiveDSN:        tradeLiveDSN,
		tradePaperLiveDSN:   tradePaperLiveDSN,
		tradePaperReplayDSN: tradePaperReplayDSN,
		status:              status,
		runtime:             quotes.NewRuntimeManager(cfg.CTP, status),
		searchRealtime:      searchRealtime,
		searchReplay:        searchReplay,
		queryRealtime:       klinequery.NewServiceWithSessionDB(realtimeDSN, sharedDSN, searchRealtime),
		queryReplay:         klinequery.NewServiceWithSessionDB(replayDSN, sharedDSN, searchReplay),
		calendar:            calendar.NewManager(sharedDSN),
		currentMode:         appmode.LiveReal,
		sessions:            make(map[string]*importer.TDXImportSession),
		wsConns:             make(map[*websocket.Conn]*wsClient),
	}
	if err := dbx.EnsureAllLogicalDatabases(cfg.DB); err != nil {
		logger.Error("ensure mysql database failed", "error", err)
	}
	if err := dbx.MigrateSharedMetaTables(cfg.DB); err != nil {
		logger.Error("migrate shared meta tables failed", "error", err)
	}
	if cfg.CTP.IsBusEnabled() {
		busPath := strings.TrimSpace(cfg.CTP.BusLogPath)
		if busPath == "" {
			busPath = filepath.Join(cfg.CTP.FlowPath, "bus")
		}
		busLog := bus.NewFileLog(busPath, time.Duration(cfg.CTP.BusFlushMS)*time.Millisecond)
		db, err := dbx.Open(replayDSN)
		if err != nil {
			logger.Error("open replay dedup db failed", "error", err)
		} else {
			store, err := bus.NewConsumerStore(db)
			if err != nil {
				logger.Error("init replay dedup store failed", "error", err)
			} else {
				s.replay = replay.NewService(busLog, store, cfg.CTP.IsReplayAllowOrderCommandDispatch())
				replayCfg := cfg.CTP
				replayCfg.DBDSN = replayDSN
				replaySink, sinkErr := quotes.NewReplaySink(replayCfg, status)
				if sinkErr != nil {
					logger.Error("init replay quotes sink failed", "error", sinkErr)
				} else {
					s.replaySink = replaySink
					s.replay.RegisterConsumer("quotes.replay_sink", replaySink.ConsumeBusEvent)
					s.replay.RegisterTaskLifecycle("quotes.replay_sink", replaySink)
				}
			}
		}
	}
	if db, err := dbx.Open(dbx.DSNForRole(cfg.DB, dbx.RoleChartUserRealtime)); err != nil {
		logger.Error("open chart layout realtime db failed", "error", err)
	} else {
		store, err := chartlayout.NewStore(db)
		if err != nil {
			logger.Error("init chart layout realtime store failed", "error", err)
		} else {
			s.chartRealtime = chartlayout.NewService(store)
		}
	}
	if db, err := dbx.Open(dbx.DSNForRole(cfg.DB, dbx.RoleChartUserReplay)); err != nil {
		logger.Error("open chart layout replay db failed", "error", err)
	} else {
		store, err := chartlayout.NewStore(db)
		if err != nil {
			logger.Error("init chart layout replay store failed", "error", err)
		} else {
			s.chartReplay = chartlayout.NewService(store)
		}
	}
	if store, err := userconfig.NewStore(sharedDSN); err != nil {
		logger.Error("init user config store failed", "error", err)
	} else {
		s.userConfig = store
		if mode, ok, loadErr := store.LoadAppMode(userconfig.DefaultOwner); loadErr != nil {
			logger.Error("load app mode failed", "error", loadErr)
		} else if ok {
			s.currentMode = appmode.Normalize(mode)
		}
	}
	if stream, err := quotes.NewChartStream(sharedDSN, status.QueueRegistry()); err != nil {
		logger.Error("init chart stream failed", "error", err)
	} else {
		s.chartStream = stream
		quotes.SetDefaultChartStream(stream)
	}
	if cfg.Strategy.IsEnabled() {
		manager, err := strategy.NewManager(cfg.Strategy, tradeLiveDSN, status.QueueRegistry())
		if err != nil {
			logger.Error("init strategy manager failed", "error", err)
		} else {
			s.strategy = manager
		}
	}
	if svc, err := trade.NewPaperService(cfg.Trade, "paper_live", tradePaperLiveDSN, status.QueueRegistry()); err != nil {
		logger.Error("init paper live trade service failed", "error", err)
	} else {
		s.tradePaperLive = svc
	}
	if svc, err := trade.NewPaperService(cfg.Trade, "paper_replay", tradePaperReplayDSN, status.QueueRegistry()); err != nil {
		logger.Error("init paper replay trade service failed", "error", err)
	} else {
		s.tradePaperReplay = svc
	}
	if cfg.Trade.IsEnabled() && s.currentAppMode() == appmode.LiveReal {
		if err := s.startTradeService(); err != nil {
			logger.Error("init trade service failed", "error", err)
		}
	}
	return s
}

func (s *Server) ListenAddr() string {
	return s.cfg.Web.ListenAddr
}

func (s *Server) Run() error {
	mux := s.Handler()
	go s.broadcastStatusTicker()
	go s.broadcastQueueTicker()
	if s.strategy != nil {
		if err := s.strategy.Start(); err != nil {
			logger.Error("start strategy manager failed", "error", err)
		}
		go s.forwardStrategyEvents()
	}
	for _, svc := range []*trade.Service{s.tradeLive, s.tradePaperLive, s.tradePaperReplay} {
		if svc == nil {
			continue
		}
		if err := svc.Start(); err != nil {
			logger.Error("start trade service failed", "error", err)
		} else {
			go s.forwardTradeEvents(svc)
		}
	}
	if s.chartStream != nil {
		go s.forwardChartEvents()
		go s.forwardQuoteEvents()
	}
	logger.Info("web server listening", "addr", s.cfg.Web.ListenAddr)
	return http.ListenAndServe(s.cfg.Web.ListenAddr, mux)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/app-mode", s.handleAppMode)
	mux.HandleFunc("/api/queues", s.handleQueues)
	mux.HandleFunc("/api/server/start", s.handleStartRuntime)
	mux.HandleFunc("/api/import/session", s.handleImportSession)
	mux.HandleFunc("/api/import/session/", s.handleImportDecision)
	mux.HandleFunc("/api/trading-sessions/import", s.handleTradingSessionsImport)
	mux.HandleFunc("/api/kline/search", s.handleKlineSearch)
	mux.HandleFunc("/api/kline/index/rebuild-current", s.handleKlineIndexRebuildCurrent)
	mux.HandleFunc("/api/kline/index/rebuild-all", s.handleKlineIndexRebuildAll)
	mux.HandleFunc("/api/kline/index/rebuild-one", s.handleKlineIndexRebuildOne)
	mux.HandleFunc("/api/kline/bars", s.handleKlineBars)
	mux.HandleFunc("/api/instruments", s.handleInstruments)
	mux.HandleFunc("/api/calendar/status", s.handleCalendarStatus)
	mux.HandleFunc("/api/calendar/import", s.handleCalendarImport)
	mux.HandleFunc("/api/calendar/import/tdx-daily", s.handleCalendarImportTDXDaily)
	mux.HandleFunc("/api/calendar/refresh", s.handleCalendarRefresh)
	mux.HandleFunc("/api/replay/start", s.handleReplayStart)
	mux.HandleFunc("/api/replay/pause", s.handleReplayPause)
	mux.HandleFunc("/api/replay/resume", s.handleReplayResume)
	mux.HandleFunc("/api/replay/stop", s.handleReplayStop)
	mux.HandleFunc("/api/replay/status", s.handleReplayStatus)
	mux.HandleFunc("/api/chart/layout", s.handleChartLayout)
	mux.HandleFunc("/api/chart/drawings", s.handleChartDrawings)
	mux.HandleFunc("/api/chart/drawings/", s.handleChartDrawingsByID)
	mux.HandleFunc("/api/strategy/status", s.handleStrategyStatus)
	mux.HandleFunc("/api/strategy/definitions", s.handleStrategyDefinitions)
	mux.HandleFunc("/api/strategy/instances", s.handleStrategyInstances)
	mux.HandleFunc("/api/strategy/instances/", s.handleStrategyInstanceAction)
	mux.HandleFunc("/api/strategy/signals", s.handleStrategySignals)
	mux.HandleFunc("/api/strategy/backtests", s.handleStrategyBacktests)
	mux.HandleFunc("/api/strategy/backtests/", s.handleStrategyBacktestByID)
	mux.HandleFunc("/api/strategy/optimize", s.handleStrategyOptimize)
	mux.HandleFunc("/api/orders/status", s.handleOrdersStatus)
	mux.HandleFunc("/api/orders/audit", s.handleOrdersAudit)
	mux.HandleFunc("/api/trade/status", s.handleTradeStatus)
	mux.HandleFunc("/api/trade/config", s.handleTradeConfig)
	mux.HandleFunc("/api/trade/account", s.handleTradeAccount)
	mux.HandleFunc("/api/trade/positions", s.handleTradePositions)
	mux.HandleFunc("/api/trade/orders", s.handleTradeOrders)
	mux.HandleFunc("/api/trade/orders/", s.handleTradeOrderAction)
	mux.HandleFunc("/api/trade/trades", s.handleTradeTrades)
	mux.HandleFunc("/api/trade/query/refresh", s.handleTradeRefresh)
	mux.HandleFunc("/api/client-log", s.handleClientLog)
	mux.HandleFunc("/ws", s.handleWS)
	mux.Handle("/", s.handleFrontend())
	return mux
}

func (s *Server) handleClientLog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 128<<10))
	if err != nil {
		http.Error(w, "read body failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var payload map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		payload = map[string]any{}
	}
	payload["received_at"] = time.Now().Format(time.RFC3339Nano)
	if payload["remote_addr"] == nil {
		payload["remote_addr"] = r.RemoteAddr
	}
	if payload["user_agent"] == nil {
		payload["user_agent"] = r.UserAgent()
	}

	line, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "encode log failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	logPath := filepath.Join("..", "ctp-future-resources", "logs", "browser.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		http.Error(w, "ensure log dir failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		http.Error(w, "open log file failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = f.Write(line)
	_, _ = f.Write([]byte("\n"))
	_ = f.Close()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) CurrentStatus() quotes.RuntimeSnapshot {
	return s.status.Snapshot(time.Now())
}

func (s *Server) currentAppMode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return appmode.Normalize(s.currentMode)
}

func (s *Server) setCurrentAppMode(mode string) {
	s.mu.Lock()
	s.currentMode = appmode.Normalize(mode)
	s.mu.Unlock()
}

func (s *Server) currentDataMode(r *http.Request) string {
	if r != nil {
		raw := strings.TrimSpace(r.URL.Query().Get("data_mode"))
		if raw == "" && r.Method != http.MethodGet && r.Body != nil {
			// no-op: non-GET endpoints decode body separately and use current mode fallback.
		}
		switch strings.ToLower(raw) {
		case "replay", "replay_paper":
			return appmode.ReplayPaper
		case "live", "live_real", "live_paper":
			return appmode.LiveReal
		}
	}
	return s.currentAppMode()
}

func (s *Server) queryForMode(mode string) *klinequery.Service {
	if appmode.UsesRealtimeMarket(mode) {
		return s.queryRealtime
	}
	return s.queryReplay
}

func (s *Server) chartForMode(mode string) *chartlayout.Service {
	if appmode.UsesRealtimeMarket(mode) {
		return s.chartRealtime
	}
	return s.chartReplay
}

func (s *Server) marketDSNForMode(mode string) string {
	if appmode.UsesRealtimeMarket(mode) {
		return s.realtimeDSN
	}
	return s.replayDSN
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]any{"status": s.status.Snapshot(time.Now())}
	if s.replay != nil {
		resp["replay"] = s.replay.Status()
	}
	if s.strategy != nil {
		resp["strategy"] = s.strategy.Status()
	}
	resp["app_mode"] = map[string]any{
		"mode":            s.currentAppMode(),
		"uses_realtime":   appmode.UsesRealtimeMarket(s.currentAppMode()),
		"replay_enabled":  s.currentAppMode() == appmode.ReplayPaper,
		"trade_backend":   s.currentTradeBackendName(),
		"chart_data_mode": s.currentChartDataMode(),
		"kline_data_mode": s.currentChartDataMode(),
	}
	resp["trade"] = s.tradeStatusSnapshot()
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	registry := s.status.QueueRegistry()
	if registry == nil {
		writeJSON(w, http.StatusOK, map[string]any{"summary": map[string]any{}, "alerts": []any{}, "queues": []any{}})
		return
	}
	writeJSON(w, http.StatusOK, registry.Snapshot())
}

func (s *Server) currentTradeBackendName() string {
	switch s.currentAppMode() {
	case appmode.LivePaper:
		return "paper_live"
	case appmode.ReplayPaper:
		return "paper_replay"
	default:
		return "live_trade"
	}
}

func (s *Server) tradeAccountIDForMode(mode string) string {
	switch appmode.Normalize(mode) {
	case appmode.LivePaper:
		return "paper_live"
	case appmode.ReplayPaper:
		return "paper_replay"
	default:
		return s.cfg.Trade.AccountID
	}
}

func (s *Server) currentChartDataMode() string {
	if appmode.UsesRealtimeMarket(s.currentAppMode()) {
		return "realtime"
	}
	return "replay"
}

func (s *Server) appModePayload() map[string]any {
	cursor, _, _ := s.userConfig.LoadReplayResumeCursor(s.currentOwner())
	return map[string]any{
		"mode":                 s.currentAppMode(),
		"uses_realtime":        appmode.UsesRealtimeMarket(s.currentAppMode()),
		"trade_backend":        s.currentTradeBackendName(),
		"chart_data_mode":      s.currentChartDataMode(),
		"kline_data_mode":      s.currentChartDataMode(),
		"replay_resume_cursor": cursor,
	}
}

func (s *Server) handleAppMode(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.appModePayload())
	case http.MethodPost:
		if s.userConfig == nil {
			http.Error(w, "user config store unavailable", http.StatusInternalServerError)
			return
		}
		var req struct {
			Mode string `json:"mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		nextMode := appmode.Normalize(req.Mode)
		prevMode := s.currentAppMode()
		if prevMode == appmode.ReplayPaper && nextMode != appmode.ReplayPaper && s.replay != nil {
			task, err := s.replay.Stop()
			if err == nil {
				cursor := task.LastCursor
				if cursor == nil {
					cursor = task.FromCursor
				}
				_ = s.userConfig.SaveReplayResumeCursor(s.currentOwner(), cursor)
			}
		}
		if err := s.userConfig.SaveAppMode(s.currentOwner(), nextMode); err != nil {
			http.Error(w, "save app mode failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		s.setCurrentAppMode(nextMode)
		if prevMode == appmode.LiveReal && nextMode != appmode.LiveReal {
			if err := s.stopTradeService(); err != nil {
				logger.Error("stop trade service on app mode switch failed", "error", err, "from_mode", prevMode, "to_mode", nextMode)
			}
		}
		if prevMode != appmode.LiveReal && nextMode == appmode.LiveReal && s.cfg.Trade.IsEnabled() {
			if err := s.startTradeService(); err != nil {
				logger.Error("start trade service on app mode switch failed", "error", err, "from_mode", prevMode, "to_mode", nextMode)
			}
		}
		payload := s.appModePayload()
		payload["ok"] = true
		writeJSON(w, http.StatusOK, payload)
		s.broadcastEvent("app_mode_update", payload)
		s.broadcastEvent("status_update", s.statusPayload())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleStartRuntime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.currentAppMode() == appmode.ReplayPaper {
		http.Error(w, "runtime start is unavailable in replay_paper mode", http.StatusBadRequest)
		return
	}
	if err := s.runtime.Start(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	s.broadcastEvent("status_update", s.statusPayload())
}

func (s *Server) handleImportSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(512 << 20); err != nil {
		http.Error(w, "parse multipart form failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	fileHeaders := r.MultipartForm.File["files"]
	if len(fileHeaders) == 0 {
		for _, values := range r.MultipartForm.File {
			fileHeaders = append(fileHeaders, values...)
		}
	}
	if len(fileHeaders) == 0 {
		http.Error(w, "no files uploaded", http.StatusBadRequest)
		return
	}
	// logger.Info("import upload received", "file_count", len(fileHeaders))

	uploadFiles := make([]importer.UploadFile, 0, len(fileHeaders))
	var totalBytes int64
	for _, fh := range fileHeaders {
		file, err := fh.Open()
		if err != nil {
			http.Error(w, "open upload file failed: "+err.Error(), http.StatusBadRequest)
			return
		}
		data, err := io.ReadAll(file)
		_ = file.Close()
		if err != nil {
			http.Error(w, "read upload file failed: "+err.Error(), http.StatusBadRequest)
			return
		}
		name := fh.Filename
		if name == "" {
			name = fh.Header.Get("X-File-Name")
		}
		size := int64(len(data))
		totalBytes += size
		// logger.Info("import upload file",
		// 	"name", name,
		// 	"size_bytes", size,
		// )
		uploadFiles = append(uploadFiles, importer.UploadFile{Name: name, Data: data})
	}
	logger.Info("import upload completed", "file_count", len(uploadFiles), "total_size_bytes", totalBytes)

	sessionID := fmt.Sprintf("%d", time.Now().UnixNano())
	mode := s.currentAppMode()
	dbPath := s.marketDSNForMode(mode)

	session := importer.NewTDXImportSessionWithOptions(sessionID, dbPath, uploadFiles, importer.ImportOptions{
		SharedDBPath:        s.sharedDSN,
		EnableMMRebuild:     true,
		AllowSessionInfer:   false,
		RequireL9:           false,
		OverwriteDuplicates: false,
	}, sessionHandler{
		server:    s,
		sessionID: sessionID,
	})
	s.mu.Lock()
	s.sessions[sessionID] = session
	s.mu.Unlock()
	session.Start()
	logger.Info("import session started", "session_id", sessionID, "file_count", len(uploadFiles))

	writeJSON(w, http.StatusOK, map[string]any{
		"session_id": sessionID,
	})
}

func (s *Server) handleTradingSessionsImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(256 << 20); err != nil {
		http.Error(w, "parse multipart form failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	fileHeaders := r.MultipartForm.File["files"]
	if len(fileHeaders) == 0 {
		for _, values := range r.MultipartForm.File {
			fileHeaders = append(fileHeaders, values...)
		}
	}
	if len(fileHeaders) == 0 {
		http.Error(w, "no files uploaded", http.StatusBadRequest)
		return
	}
	uploadFiles := make([]importer.UploadFile, 0, len(fileHeaders))
	for _, fh := range fileHeaders {
		file, err := fh.Open()
		if err != nil {
			http.Error(w, "open upload file failed: "+err.Error(), http.StatusBadRequest)
			return
		}
		data, err := io.ReadAll(file)
		_ = file.Close()
		if err != nil {
			http.Error(w, "read upload file failed: "+err.Error(), http.StatusBadRequest)
			return
		}
		name := fh.Filename
		if name == "" {
			name = fh.Header.Get("X-File-Name")
		}
		uploadFiles = append(uploadFiles, importer.UploadFile{Name: name, Data: data})
	}
	mode := s.currentAppMode()
	result, err := importer.ImportTradingSessions(fmt.Sprintf("trading-sessions-%d", time.Now().UnixNano()), s.marketDSNForMode(mode), s.sharedDSN, uploadFiles)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.searchRealtime != nil {
		s.searchRealtime.Invalidate()
	}
	if s.searchReplay != nil {
		s.searchReplay.Invalidate()
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleImportDecision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/import/session/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[1] != "decision" || parts[0] == "" {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	sessionID := parts[0]

	var req importer.DecisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}

	session, err := s.getSession(sessionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err := session.ApplyDecision(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.wsConns[conn] = &wsClient{subs: make(map[string]quotes.ChartSubscription), quoteSubs: make(map[string]quotes.ChartSubscription)}
	s.mu.Unlock()

	s.writeConnJSON(conn, map[string]any{
		"type": "status_update",
		"data": s.statusPayload(),
	})

	go func() {
		defer func() {
			s.removeWSConn(conn)
			conn.Close()
		}()
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			s.handleWSMessage(conn, raw)
		}
	}()
}

func (s *Server) writeConnJSON(conn *websocket.Conn, payload any) error {
	s.wsWriteMu.Lock()
	defer s.wsWriteMu.Unlock()
	return s.writeConnJSONLocked(conn, payload)
}

func (s *Server) writeConnJSONLocked(conn *websocket.Conn, payload any) error {
	return conn.WriteJSON(payload)
}

func (s *Server) removeWSConn(conn *websocket.Conn) {
	var subs []quotes.ChartSubscription
	var quoteSubs []quotes.ChartSubscription
	s.mu.Lock()
	if client, ok := s.wsConns[conn]; ok && client != nil {
		subs = make([]quotes.ChartSubscription, 0, len(client.subs))
		for _, sub := range client.subs {
			subs = append(subs, sub)
		}
		quoteSubs = make([]quotes.ChartSubscription, 0, len(client.quoteSubs))
		for _, sub := range client.quoteSubs {
			quoteSubs = append(quoteSubs, sub)
		}
	}
	delete(s.wsConns, conn)
	s.mu.Unlock()
	if s.chartStream != nil {
		for _, sub := range subs {
			s.chartStream.RemoveInterest(sub)
		}
		for _, sub := range quoteSubs {
			s.chartStream.RemoveQuoteInterest(sub)
		}
	}
}

func (s *Server) handleWSMessage(conn *websocket.Conn, raw []byte) {
	var msg struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	switch strings.TrimSpace(msg.Type) {
	case "chart_subscribe":
		s.handleChartSubscribe(conn, msg.Data)
	case "chart_unsubscribe":
		s.handleChartUnsubscribe(conn, msg.Data)
	case "quote_subscribe":
		s.handleQuoteSubscribe(conn, msg.Data)
	case "quote_unsubscribe":
		s.handleQuoteUnsubscribe(conn, msg.Data)
	case "chart_ping":
		_ = s.writeConnJSON(conn, map[string]any{"type": "chart_pong", "data": map[string]any{}})
	}
}

func (s *Server) handleChartSubscribe(conn *websocket.Conn, raw json.RawMessage) {
	var req quotes.ChartSubscription
	if err := json.Unmarshal(raw, &req); err != nil {
		s.writeChartSubscriptionError(conn, req, "invalid subscribe payload")
		return
	}
	if s.chartStream == nil {
		s.writeChartSubscriptionError(conn, req, "chart stream is not available")
		return
	}
	sub, err := s.chartStream.AddInterest(req)
	if err != nil {
		s.writeChartSubscriptionError(conn, req, err.Error())
		return
	}
	key := quotes.ChartSubscriptionKey(sub)
	var added bool
	s.mu.Lock()
	client := s.wsConns[conn]
	if client == nil {
		client = &wsClient{subs: make(map[string]quotes.ChartSubscription), quoteSubs: make(map[string]quotes.ChartSubscription)}
		s.wsConns[conn] = client
	}
	if _, exists := client.subs[key]; !exists {
		client.subs[key] = sub
		added = true
	}
	s.mu.Unlock()
	logger.Info("chart subscribe",
		"subscription_key", key,
		"added", added,
		"symbol", sub.Symbol,
		"type", sub.Type,
		"variety", sub.Variety,
		"timeframe", sub.Timeframe,
	)
	if !added {
		s.chartStream.RemoveInterest(sub)
		return
	}
	if update, ok := s.chartStream.SnapshotQuote(sub); ok {
		s.broadcastQuoteUpdate(update)
	}
}

func (s *Server) handleChartUnsubscribe(conn *websocket.Conn, raw json.RawMessage) {
	var req quotes.ChartSubscription
	if err := json.Unmarshal(raw, &req); err != nil {
		return
	}
	sub, err := quotes.NormalizeChartSubscription(req)
	if err != nil {
		return
	}
	key := quotes.ChartSubscriptionKey(sub)
	var removed bool
	s.mu.Lock()
	if client := s.wsConns[conn]; client != nil {
		if _, exists := client.subs[key]; exists {
			delete(client.subs, key)
			removed = true
		}
	}
	s.mu.Unlock()
	logger.Info("chart unsubscribe",
		"subscription_key", key,
		"removed", removed,
		"symbol", sub.Symbol,
		"type", sub.Type,
		"variety", sub.Variety,
		"timeframe", sub.Timeframe,
	)
	if removed && s.chartStream != nil {
		s.chartStream.RemoveInterest(sub)
	}
}

func (s *Server) handleQuoteSubscribe(conn *websocket.Conn, raw json.RawMessage) {
	var req quotes.ChartSubscription
	if err := json.Unmarshal(raw, &req); err != nil {
		s.writeChartSubscriptionError(conn, req, "invalid subscribe payload")
		return
	}
	if s.chartStream == nil {
		s.writeChartSubscriptionError(conn, req, "chart stream is not available")
		return
	}
	sub, err := s.chartStream.AddQuoteInterest(req)
	if err != nil {
		s.writeChartSubscriptionError(conn, req, err.Error())
		return
	}
	key := quotes.ChartSubscriptionKey(sub)
	var added bool
	s.mu.Lock()
	client := s.wsConns[conn]
	if client == nil {
		client = &wsClient{subs: make(map[string]quotes.ChartSubscription), quoteSubs: make(map[string]quotes.ChartSubscription)}
		s.wsConns[conn] = client
	}
	if _, exists := client.quoteSubs[key]; !exists {
		client.quoteSubs[key] = sub
		added = true
	}
	s.mu.Unlock()
	logger.Info("quote subscribe",
		"subscription_key", key,
		"added", added,
		"symbol", sub.Symbol,
		"type", sub.Type,
		"variety", sub.Variety,
		"timeframe", sub.Timeframe,
	)
	if !added {
		s.chartStream.RemoveQuoteInterest(sub)
		return
	}
	if update, ok := s.chartStream.SnapshotQuote(sub); ok {
		s.broadcastQuoteUpdate(update)
	}
}

func (s *Server) handleQuoteUnsubscribe(conn *websocket.Conn, raw json.RawMessage) {
	var req quotes.ChartSubscription
	if err := json.Unmarshal(raw, &req); err != nil {
		return
	}
	sub, err := quotes.NormalizeChartSubscription(req)
	if err != nil {
		return
	}
	key := quotes.ChartSubscriptionKey(sub)
	var removed bool
	s.mu.Lock()
	if client := s.wsConns[conn]; client != nil {
		if _, exists := client.quoteSubs[key]; exists {
			delete(client.quoteSubs, key)
			removed = true
		}
	}
	s.mu.Unlock()
	logger.Info("quote unsubscribe",
		"subscription_key", key,
		"removed", removed,
		"symbol", sub.Symbol,
		"type", sub.Type,
		"variety", sub.Variety,
		"timeframe", sub.Timeframe,
	)
	if removed && s.chartStream != nil {
		s.chartStream.RemoveQuoteInterest(sub)
	}
}

func (s *Server) writeChartSubscriptionError(conn *websocket.Conn, sub quotes.ChartSubscription, message string) {
	_ = s.writeConnJSON(conn, map[string]any{
		"type": "chart_subscription_error",
		"data": map[string]any{
			"subscription": sub,
			"message":      message,
		},
	})
}

func (s *Server) handleKlineSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	keyword := strings.TrimSpace(r.URL.Query().Get("keyword"))
	page, pageSize := parsePageArgs(r.URL.Query().Get("page"), r.URL.Query().Get("page_size"))

	querySvc := s.queryForMode(s.currentAppMode())
	resp, err := querySvc.Search(keyword, page, pageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleKlineIndexRebuildCurrent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Items []klinequery.SearchItem `json:"items"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	querySvc := s.queryForMode(s.currentAppMode())
	if err := querySvc.RebuildSearchItems(req.Items); err != nil {
		if errors.Is(err, searchindex.ErrBusy) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleKlineIndexRebuildAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	querySvc := s.queryForMode(s.currentAppMode())
	if err := querySvc.RebuildAllIndex(); err != nil {
		if errors.Is(err, searchindex.ErrBusy) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleKlineIndexRebuildOne(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req klinequery.SearchItem
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	querySvc := s.queryForMode(s.currentAppMode())
	item, exists, err := querySvc.RebuildSearchItem(req)
	if err != nil {
		if errors.Is(err, searchindex.ErrBusy) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"exists": exists,
		"item":   item,
	})
}

func (s *Server) handleKlineBars(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	symbol := strings.TrimSpace(r.URL.Query().Get("symbol"))
	kind := strings.TrimSpace(r.URL.Query().Get("type"))
	if kind == "" {
		kind = inferKlineTypeBySymbol(symbol)
	}
	variety := strings.TrimSpace(r.URL.Query().Get("variety"))
	timeframe := strings.TrimSpace(r.URL.Query().Get("timeframe"))
	if timeframe == "" {
		timeframe = "1m"
	}
	if symbol == "" {
		http.Error(w, "symbol is required", http.StatusBadRequest)
		return
	}
	end, err := parseOptionalEndTime(r.URL.Query().Get("end"))
	if err != nil {
		http.Error(w, "invalid end: "+err.Error(), http.StatusBadRequest)
		return
	}
	limit := parseLimitArg(r.URL.Query().Get("limit"), 2000, 5000)
	logger.Info("api kline bars request",
		"symbol", symbol,
		"type", kind,
		"variety", variety,
		"timeframe", timeframe,
		"end", end.Format("2006-01-02 15:04:00"),
		"limit", limit,
		"raw_query", r.URL.RawQuery,
	)
	mode := s.currentDataMode(r)
	resp, err := s.queryForMode(mode).BarsByEnd(symbol, kind, variety, timeframe, end, limit)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			logger.Info("api kline bars no rows", "symbol", symbol, "type", kind, "variety", variety)
			http.Error(w, "symbol not found", http.StatusNotFound)
			return
		}
		if errors.Is(err, klinequery.ErrInvalidTimeframe) {
			logger.Info("api kline bars bad request", "error_class", "invalid_timeframe", "symbol", symbol, "type", kind, "variety", variety, "timeframe", timeframe, "error", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if errors.Is(err, klinequery.ErrTradingSessionNotReady) {
			logger.Info("api kline bars unprocessable", "error_class", "session_not_ready", "symbol", symbol, "type", kind, "variety", variety, "timeframe", timeframe, "error", err)
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		logger.Error("api kline bars failed", "error_class", "internal", "symbol", symbol, "type", kind, "variety", variety, "timeframe", timeframe, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.Info("api kline bars success",
		"symbol", symbol,
		"type", kind,
		"variety", variety,
		"bar_count", len(resp.Bars),
	)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleInstruments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	page, pageSize := parsePageArgs(r.URL.Query().Get("page"), r.URL.Query().Get("page_size"))
	resp, err := s.queryForMode(s.currentAppMode()).ListContracts(page, pageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleCalendarStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	st, err := s.calendar.Status(s.cfg.Calendar.MinFutureOpenDays)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleCalendarRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	err := s.calendar.RefreshIfNeeded(calendar.Config{
		AutoUpdateOnStart:  true,
		MinFutureOpenDays:  s.cfg.Calendar.MinFutureOpenDays,
		SourceURL:          s.cfg.Calendar.SourceURL,
		SourceCSVPath:      s.cfg.Calendar.SourceCSVPath,
		CheckIntervalHours: 0,
		BrowserFallback:    s.cfg.Calendar.IsBrowserFallbackEnabled(),
		BrowserPath:        s.cfg.Calendar.BrowserPath,
		BrowserHeadless:    s.cfg.Calendar.IsBrowserHeadless(),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	st, err := s.calendar.Status(s.cfg.Calendar.MinFutureOpenDays)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleCalendarImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		http.Error(w, "parse multipart form failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	var fh *multipart.FileHeader
	if list := r.MultipartForm.File["file"]; len(list) > 0 {
		fh = list[0]
	} else {
		for _, list := range r.MultipartForm.File {
			if len(list) > 0 {
				fh = list[0]
				break
			}
		}
	}
	if fh == nil {
		http.Error(w, "file is required", http.StatusBadRequest)
		return
	}
	f, err := fh.Open()
	if err != nil {
		http.Error(w, "open file failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		http.Error(w, "read file failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.calendar.ImportCSVBytes(data, "api_upload:"+fh.Filename); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	st, err := s.calendar.Status(s.cfg.Calendar.MinFutureOpenDays)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleCalendarImportTDXDaily(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		http.Error(w, "parse multipart form failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	var fileHeaders []*multipart.FileHeader
	for _, list := range r.MultipartForm.File {
		fileHeaders = append(fileHeaders, list...)
	}
	if len(fileHeaders) != 1 {
		http.Error(w, "file is required and only one file is allowed", http.StatusBadRequest)
		return
	}
	fh := fileHeaders[0]
	f, err := fh.Open()
	if err != nil {
		http.Error(w, "open file failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		http.Error(w, "read file failed: "+err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.calendar.ImportTDXDailyBytes(data, "api_upload_tdx_daily:"+fh.Filename)
	if err != nil {
		logger.Error("api calendar import tdx daily failed", "file", fh.Filename, "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logger.Info("api calendar import tdx daily success",
		"file", fh.Filename,
		"first_trading_day", result.MinDate,
		"last_trading_day", result.MaxDate,
		"total_trading_days", result.ImportedDays,
		"target_table", result.TableName,
		"write_success", result.WriteSuccess,
	)
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleFrontend() http.Handler {
	distDir := filepath.Join("web", "dist")
	indexPath := filepath.Join(distDir, "index.html")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/ws" {
			http.NotFound(w, r)
			return
		}
		requestPath := strings.TrimPrefix(filepath.Clean(r.URL.Path), string(filepath.Separator))
		if requestPath == "." || requestPath == "" {
			s.serveIndexWithWebDefaults(w, r, indexPath)
			return
		}
		full := filepath.Join(distDir, requestPath)
		if info, err := os.Stat(full); err == nil && !info.IsDir() {
			http.ServeFile(w, r, full)
			return
		}
		s.serveIndexWithWebDefaults(w, r, indexPath)
	})
}

func (s *Server) serveIndexWithWebDefaults(w http.ResponseWriter, r *http.Request, indexPath string) {
	content, err := os.ReadFile(indexPath)
	if err != nil {
		http.ServeFile(w, r, indexPath)
		return
	}
	defaults := map[string]int{
		"draw_debug":  0,
		"browser_log": 0,
	}
	if s.cfg.Web.IsDrawDebugDefaultEnabled() {
		defaults["draw_debug"] = 1
	}
	if s.cfg.Web.IsBrowserLogDefaultEnabled() {
		defaults["browser_log"] = 1
	}
	raw, err := json.Marshal(defaults)
	if err != nil {
		http.ServeFile(w, r, indexPath)
		return
	}
	snippet := []byte("<script>window.__WEB_DEBUG_DEFAULTS__=" + string(raw) + ";</script>")
	needle := []byte("</head>")
	if pos := bytes.Index(content, needle); pos >= 0 {
		out := make([]byte, 0, len(content)+len(snippet))
		out = append(out, content[:pos]...)
		out = append(out, snippet...)
		out = append(out, content[pos:]...)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(out)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(append(content, snippet...))
}

func (s *Server) handleReplayStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.replay == nil {
		http.Error(w, "replay is disabled", http.StatusBadRequest)
		return
	}
	if s.currentAppMode() != appmode.ReplayPaper {
		http.Error(w, "replay is only available in replay_paper mode", http.StatusBadRequest)
		return
	}
	var req replay.StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Mode) == "" {
		req.Mode = s.cfg.CTP.ReplayDefaultMode
	}
	if req.Speed == 0 {
		req.Speed = s.cfg.CTP.ReplayDefaultSpeed
	}
	if strings.TrimSpace(req.TickDir) == "" {
		req.TickDir = filepath.Join(s.cfg.CTP.FlowPath, "ticks")
	}
	if s.replaySink != nil {
		if err := s.replaySink.PrepareReplayWindow(req); err != nil {
			http.Error(w, "prepare replay window failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if !req.FullReplay {
		logger.Info("replay start requested without full replay", "tick_dir", req.TickDir)
	}
	task, err := s.replay.Start(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, replay.StartResponse{OK: true, Task: task})
}

func (s *Server) handleReplayPause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.replay == nil {
		http.Error(w, "replay is disabled", http.StatusBadRequest)
		return
	}
	task, err := s.replay.Pause()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, replay.ActionResponse{OK: true, Task: task})
}

func (s *Server) handleReplayResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.replay == nil {
		http.Error(w, "replay is disabled", http.StatusBadRequest)
		return
	}
	task, err := s.replay.Resume()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, replay.ActionResponse{OK: true, Task: task})
}

func (s *Server) handleReplayStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.replay == nil {
		http.Error(w, "replay is disabled", http.StatusBadRequest)
		return
	}
	task, err := s.replay.Stop()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, replay.ActionResponse{OK: true, Task: task})
}

func (s *Server) handleReplayStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.replay == nil {
		http.Error(w, "replay is disabled", http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, replay.StatusResponse{Task: s.replay.Status()})
}

func (s *Server) handleChartLayout(w http.ResponseWriter, r *http.Request) {
	mode := s.currentDataMode(r)
	chartSvc := s.chartForMode(mode)
	if chartSvc == nil {
		http.Error(w, "chart layout service unavailable", http.StatusInternalServerError)
		return
	}
	switch r.Method {
	case http.MethodGet:
		owner := strings.TrimSpace(r.URL.Query().Get("owner"))
		if owner == "" {
			owner = "admin"
		}
		symbol := strings.TrimSpace(r.URL.Query().Get("symbol"))
		kind := strings.TrimSpace(r.URL.Query().Get("type"))
		variety := strings.TrimSpace(r.URL.Query().Get("variety"))
		timeframe := strings.TrimSpace(r.URL.Query().Get("timeframe"))
		logger.Info("api chart layout get", "owner", owner, "symbol", symbol, "type", kind, "variety", variety, "timeframe", timeframe)
		layout, ok, err := chartSvc.Get(owner, symbol, kind, variety, timeframe)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !ok {
			writeJSON(w, http.StatusOK, chartlayout.LayoutSnapshot{
				Owner:     owner,
				Symbol:    symbol,
				Type:      kind,
				Variety:   variety,
				Timeframe: timeframe,
				Theme:     "dark",
				Panes: chartlayout.PaneSettings{
					RightWatchlistOpen: true,
					BottomPanelOpen:    true,
				},
				Indicators: chartlayout.IndicatorSettings{
					MA20:   true,
					MACD:   true,
					Volume: true,
				},
				Channels: chartlayout.ChannelLayout{
					Settings:   chartlayout.DefaultChannelSettings(),
					Decisions:  []chartlayout.ChannelDecision{},
					SelectedID: "",
				},
				Reversal: chartlayout.ReversalLayout{
					Settings:       chartlayout.DefaultReversalSettings(),
					Results:        chartlayout.ReversalResult{Lines: []chartlayout.ReversalLine{}, Events: []chartlayout.ReversalEvent{}},
					PersistVersion: 0,
					SelectedID:     "",
				},
				Drawings: []chartlayout.DrawingObject{},
			})
			return
		}
		writeJSON(w, http.StatusOK, layout)
	case http.MethodPut:
		var req chartlayout.LayoutSnapshot
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		logger.Info("api chart layout put", "owner", req.Owner, "symbol", req.Symbol, "type", req.Type, "variety", req.Variety, "timeframe", req.Timeframe, "drawing_count", len(req.Drawings))
		out, err := chartSvc.Put(req)
		if err != nil {
			// Self-heal for legacy schema: retry once after ensuring latest MySQL schema.
			if strings.Contains(err.Error(), "Unknown column 'owner'") || strings.Contains(err.Error(), "Unknown column `owner`") {
				logger.Info("api chart layout put schema mismatch detected, attempting schema ensure and retry")
				dsn := dbx.DSNForRole(s.cfg.DB, dbx.RoleChartUserRealtime)
				if mode == appmode.ReplayPaper {
					dsn = dbx.DSNForRole(s.cfg.DB, dbx.RoleChartUserReplay)
				}
				if db, openErr := dbx.Open(dsn); openErr != nil {
					logger.Error("api chart layout put schema self-heal open db failed", "error", openErr)
				} else {
					roleCfg := dbx.ConfigForRole(s.cfg.DB, dbx.RoleChartUserRealtime)
					roleName := dbx.RoleChartUserRealtime
					if mode == appmode.ReplayPaper {
						roleCfg = dbx.ConfigForRole(s.cfg.DB, dbx.RoleChartUserReplay)
						roleName = dbx.RoleChartUserReplay
					}
					if ensureErr := dbx.EnsureDatabaseAndSchemaForRole(roleCfg, roleName, db); ensureErr != nil {
						logger.Error("api chart layout put schema self-heal ensure failed", "error", ensureErr)
					} else {
						logger.Info("api chart layout put schema self-heal ensure success")
						out, err = chartSvc.Put(req)
					}
					_ = db.Close()
				}
			}
		}
		if err != nil {
			logger.Error("api chart layout put failed",
				"owner", req.Owner,
				"symbol", req.Symbol,
				"type", req.Type,
				"variety", req.Variety,
				"timeframe", req.Timeframe,
				"error", err,
			)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		logger.Info("api chart layout put success",
			"owner", out.Owner,
			"symbol", out.Symbol,
			"type", out.Type,
			"variety", out.Variety,
			"timeframe", out.Timeframe,
			"drawing_count", len(out.Drawings),
		)
		writeJSON(w, http.StatusOK, out)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleChartDrawings(w http.ResponseWriter, r *http.Request) {
	chartSvc := s.chartForMode(s.currentDataMode(r))
	if chartSvc == nil {
		http.Error(w, "chart layout service unavailable", http.StatusInternalServerError)
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req chartlayout.DrawingObject
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	out, err := chartSvc.UpsertDrawing(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleChartDrawingsByID(w http.ResponseWriter, r *http.Request) {
	chartSvc := s.chartForMode(s.currentDataMode(r))
	if chartSvc == nil {
		http.Error(w, "chart layout service unavailable", http.StatusInternalServerError)
		return
	}
	id := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/chart/drawings/"))
	if id == "" {
		http.Error(w, "drawing id is required", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	symbol := strings.TrimSpace(r.URL.Query().Get("symbol"))
	owner := strings.TrimSpace(r.URL.Query().Get("owner"))
	kind := strings.TrimSpace(r.URL.Query().Get("type"))
	variety := strings.TrimSpace(r.URL.Query().Get("variety"))
	timeframe := strings.TrimSpace(r.URL.Query().Get("timeframe"))
	ok, err := chartSvc.DeleteDrawing(owner, symbol, kind, variety, timeframe, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		http.Error(w, "drawing not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) requireStrategy(w http.ResponseWriter) *strategy.Manager {
	if s.strategy == nil {
		http.Error(w, "strategy service unavailable", http.StatusNotFound)
		return nil
	}
	return s.strategy
}

func (s *Server) handleStrategyStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	manager := s.requireStrategy(w)
	if manager == nil {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": manager.Status()})
}

func (s *Server) handleStrategyDefinitions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	manager := s.requireStrategy(w)
	if manager == nil {
		return
	}
	items, err := manager.ListDefinitions()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleStrategyInstances(w http.ResponseWriter, r *http.Request) {
	manager := s.requireStrategy(w)
	if manager == nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := manager.ListInstances()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost, http.MethodPut:
		var req strategy.StrategyInstance
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(req.StrategyID) == "" {
			http.Error(w, "strategy_id is required", http.StatusBadRequest)
			return
		}
		if err := manager.SaveInstance(req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "instance_id": req.InstanceID})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleStrategyInstanceAction(w http.ResponseWriter, r *http.Request) {
	manager := s.requireStrategy(w)
	if manager == nil {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/strategy/instances/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var err error
	switch parts[1] {
	case "start":
		err = manager.StartInstance(parts[0])
	case "stop":
		err = manager.StopInstance(parts[0])
	default:
		http.Error(w, "invalid action", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleStrategySignals(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	manager := s.requireStrategy(w)
	if manager == nil {
		return
	}
	items, err := manager.ListSignals(parseLimitArg(r.URL.Query().Get("limit"), 100, 200))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleStrategyBacktests(w http.ResponseWriter, r *http.Request) {
	manager := s.requireStrategy(w)
	if manager == nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := manager.ListRuns(parseLimitArg(r.URL.Query().Get("limit"), 50, 200))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		var req strategy.BacktestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		run, err := manager.RunBacktest(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSON(w, http.StatusOK, run)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleStrategyBacktestByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	manager := s.requireStrategy(w)
	if manager == nil {
		return
	}
	runID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/strategy/backtests/"))
	if runID == "" {
		http.Error(w, "run id is required", http.StatusBadRequest)
		return
	}
	run, err := manager.GetRun(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleStrategyOptimize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	manager := s.requireStrategy(w)
	if manager == nil {
		return
	}
	var req strategy.ParameterSweepRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	run, err := manager.RunParameterSweep(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleOrdersStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	manager := s.requireStrategy(w)
	if manager == nil {
		return
	}
	writeJSON(w, http.StatusOK, manager.OrdersStatus())
}

func (s *Server) handleOrdersAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	manager := s.requireStrategy(w)
	if manager == nil {
		return
	}
	items, err := manager.ListOrderAudits(parseLimitArg(r.URL.Query().Get("limit"), 100, 200))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) requireTrade(w http.ResponseWriter) *trade.Service {
	svc := s.getTradeService()
	if svc == nil {
		http.Error(w, "trade service unavailable", http.StatusNotFound)
		return nil
	}
	return svc
}

func (s *Server) currentOwner() string {
	return userconfig.DefaultOwner
}

func (s *Server) getTradeService() *trade.Service {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch appmode.Normalize(s.currentMode) {
	case appmode.LivePaper:
		return s.tradePaperLive
	case appmode.ReplayPaper:
		return s.tradePaperReplay
	default:
		return s.tradeLive
	}
}

func (s *Server) tradeStatusSnapshot() trade.TradeStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch appmode.Normalize(s.currentMode) {
	case appmode.LivePaper:
		if s.tradePaperLive != nil {
			return s.tradePaperLive.Status()
		}
	case appmode.ReplayPaper:
		if s.tradePaperReplay != nil {
			return s.tradePaperReplay.Status()
		}
	default:
		if s.tradeLive != nil {
			return s.tradeLive.Status()
		}
	}
	return trade.TradeStatus{
		Enabled:             appmode.Normalize(s.currentMode) != appmode.LiveReal || s.cfg.Trade.IsEnabled(),
		AccountID:           s.tradeAccountIDForMode(appmode.Normalize(s.currentMode)),
		TraderFront:         appmode.Normalize(s.currentMode) != appmode.LiveReal,
		TraderLogin:         appmode.Normalize(s.currentMode) != appmode.LiveReal,
		SettlementConfirmed: appmode.Normalize(s.currentMode) != appmode.LiveReal,
		UpdatedAt:           time.Now(),
	}
}

func (s *Server) startTradeService() error {
	if s.currentAppMode() != appmode.LiveReal {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tradeLive != nil {
		return nil
	}
	svc, err := trade.NewService(s.cfg.Trade, s.cfg.CTP, s.tradeLiveDSN, s.status.QueueRegistry())
	if err != nil {
		return err
	}
	if err := svc.Start(); err != nil {
		_ = svc.Close()
		return err
	}
	s.tradeLive = svc
	go s.forwardTradeEvents(svc)
	return nil
}

func (s *Server) stopTradeService() error {
	s.mu.Lock()
	svc := s.tradeLive
	s.tradeLive = nil
	s.mu.Unlock()
	if svc == nil {
		return nil
	}
	return svc.Close()
}

func (s *Server) handleTradeStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": s.tradeStatusSnapshot()})
}

func (s *Server) handleTradeConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"owner":  s.currentOwner(),
			"status": s.tradeStatusSnapshot(),
		})
	case http.MethodPost:
		if s.userConfig == nil {
			http.Error(w, "user config store unavailable", http.StatusInternalServerError)
			return
		}
		var req struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		if err := s.userConfig.SaveTradeEnabled(s.currentOwner(), req.Enabled); err != nil {
			http.Error(w, "save user config failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		s.mu.Lock()
		s.cfg.Trade.Enabled = &req.Enabled
		s.mu.Unlock()
		var actionErr error
		if req.Enabled {
			actionErr = s.startTradeService()
		} else {
			actionErr = s.stopTradeService()
		}
		status := s.tradeStatusSnapshot()
		if actionErr != nil {
			status.LastError = actionErr.Error()
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"owner":  s.currentOwner(),
				"status": status,
			})
			return
		}
		s.broadcastEvent("trade_status_update", status)
		s.broadcastEvent("status_update", s.statusPayload())
		writeJSON(w, http.StatusOK, map[string]any{
			"owner":  s.currentOwner(),
			"status": status,
		})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTradeAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	svc := s.requireTrade(w)
	if svc == nil {
		return
	}
	item, err := svc.Account()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) handleTradePositions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	svc := s.requireTrade(w)
	if svc == nil {
		return
	}
	items, err := svc.Positions()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleTradeOrders(w http.ResponseWriter, r *http.Request) {
	svc := s.requireTrade(w)
	if svc == nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := svc.Orders(parseLimitArg(r.URL.Query().Get("limit"), 100, 500))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		var req trade.SubmitOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(req.AccountID) == "" {
			req.AccountID = s.cfg.Trade.AccountID
		}
		rec, err := svc.SubmitOrder(r.Context(), req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, rec)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTradeOrderAction(w http.ResponseWriter, r *http.Request) {
	svc := s.requireTrade(w)
	if svc == nil {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/trade/orders/")
	parts := strings.Split(path, "/")
	if len(parts) < 1 || strings.TrimSpace(parts[0]) == "" {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	commandID := strings.TrimSpace(parts[0])
	if len(parts) == 1 && r.Method == http.MethodGet {
		item, err := svc.Order(commandID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, item)
		return
	}
	if len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost {
		var req trade.CancelOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		req.CommandID = commandID
		if strings.TrimSpace(req.AccountID) == "" {
			req.AccountID = s.cfg.Trade.AccountID
		}
		rec, err := svc.CancelOrder(r.Context(), req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, rec)
		return
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (s *Server) handleTradeTrades(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	svc := s.requireTrade(w)
	if svc == nil {
		return
	}
	items, err := svc.Trades(parseLimitArg(r.URL.Query().Get("limit"), 100, 500))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleTradeRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	svc := s.requireTrade(w)
	if svc == nil {
		return
	}
	if err := svc.RefreshAll(); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) forwardStrategyEvents() {
	if s.strategy == nil {
		return
	}
	ch, cancel := s.strategy.Subscribe()
	defer cancel()
	for ev := range ch {
		s.broadcastEvent(ev.Type, ev.Data)
	}
}

func (s *Server) forwardTradeEvents(svc *trade.Service) {
	if svc == nil {
		return
	}
	ch, cancel := svc.Subscribe()
	defer cancel()
	for ev := range ch {
		s.broadcastEvent(ev.Type, ev.Data)
	}
}

func (s *Server) forwardChartEvents() {
	if s.chartStream == nil {
		return
	}
	ch, cancel := s.chartStream.Subscribe()
	defer cancel()
	for update := range ch {
		s.broadcastChartUpdate(update)
	}
}

func (s *Server) forwardQuoteEvents() {
	if s.chartStream == nil {
		return
	}
	ch, cancel := s.chartStream.SubscribeQuotes()
	defer cancel()
	for update := range ch {
		s.broadcastQuoteUpdate(update)
	}
}

func (s *Server) broadcastStatusTicker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.broadcastEvent("status_update", s.statusPayload())
	}
}

func (s *Server) broadcastQueueTicker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if s.status == nil || s.status.QueueRegistry() == nil {
			continue
		}
		s.broadcastEvent("queue_status_update", s.status.QueueRegistry().Snapshot())
	}
}

func (s *Server) statusPayload() map[string]any {
	out := map[string]any{
		"status": s.status.Snapshot(time.Now()),
	}
	if s.replay != nil {
		out["replay"] = s.replay.Status()
	}
	if s.strategy != nil {
		out["strategy"] = s.strategy.Status()
	}
	out["trade"] = s.tradeStatusSnapshot()
	return out
}

func (s *Server) broadcastEvent(eventType string, data any) {
	payload := map[string]any{
		"type": eventType,
		"data": data,
	}

	s.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(s.wsConns))
	for conn := range s.wsConns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	s.wsWriteMu.Lock()
	defer s.wsWriteMu.Unlock()

	for _, conn := range conns {
		if err := s.writeConnJSONLocked(conn, payload); err != nil {
			s.removeWSConn(conn)
			_ = conn.Close()
		}
	}
}

func (s *Server) broadcastChartUpdate(update quotes.ChartBarUpdate) {
	broadcastChartUpdateRateProbe.Inc()
	payload := map[string]any{
		"type": "chart_bar_update",
		"data": update,
	}
	key := quotes.ChartSubscriptionKey(update.Subscription)
	s.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(s.wsConns))
	for conn, client := range s.wsConns {
		if client == nil {
			continue
		}
		if _, ok := client.subs[key]; ok {
			conns = append(conns, conn)
		}
	}
	s.mu.Unlock()
	broadcastChartUpdateKeyRateProbe.Add(key, len(conns))
	for _, conn := range conns {
		if err := s.writeConnJSON(conn, payload); err != nil {
			s.removeWSConn(conn)
			_ = conn.Close()
		}
	}
}

func (s *Server) broadcastQuoteUpdate(update quotes.ChartQuoteUpdate) {
	payload := map[string]any{
		"type": "quote_snapshot_update",
		"data": update,
	}
	key := quotes.ChartSubscriptionKey(update.Subscription)
	s.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(s.wsConns))
	for conn, client := range s.wsConns {
		if client == nil {
			continue
		}
		if _, ok := client.quoteSubs[key]; ok {
			conns = append(conns, conn)
		}
	}
	s.mu.Unlock()
	for _, conn := range conns {
		if err := s.writeConnJSON(conn, payload); err != nil {
			s.removeWSConn(conn)
			_ = conn.Close()
		}
	}
}

func (s *Server) getSession(id string) (*importer.TDXImportSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[id]
	if !ok {
		return nil, errors.New("session not found")
	}
	return session, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

type sessionHandler struct {
	// server 用于把导入过程中的进度和冲突事件广播到前端。
	server *Server
	// sessionID 标识当前导入会话，便于前后端关联事件。
	sessionID string
}

func (h sessionHandler) OnProgress(p importer.Progress) {
	logger.Info("import session progress",
		"session_id", h.sessionID,
		"processed_files", p.ProcessedFiles,
		"total_files", p.TotalFiles,
		"total_lines", p.TotalLines,
		"inserted_rows", p.InsertedRows,
		"overwritten_rows", p.OverwrittenRows,
		"skipped_rows", p.SkippedRows,
		"error_count", p.ErrorCount,
	)
	h.server.broadcastEvent("import_progress", map[string]any{
		"session_id": h.sessionID,
		"progress":   p,
	})
}

func (h sessionHandler) OnConflict(c importer.ConflictRecord) {
	h.server.broadcastEvent("import_conflict", map[string]any{
		"session_id": h.sessionID,
		"conflict":   c,
	})
}

func (h sessionHandler) OnDone(p importer.Progress) {
	if h.server.searchRealtime != nil {
		h.server.searchRealtime.Invalidate()
	}
	if h.server.searchReplay != nil {
		h.server.searchReplay.Invalidate()
	}
	logger.Info("import session done",
		"session_id", h.sessionID,
		"processed_files", p.ProcessedFiles,
		"total_files", p.TotalFiles,
		"total_lines", p.TotalLines,
		"inserted_rows", p.InsertedRows,
		"overwritten_rows", p.OverwrittenRows,
		"skipped_rows", p.SkippedRows,
		"skipped_files", p.SkippedFiles,
		"error_count", p.ErrorCount,
		"canceled", p.Canceled,
	)
	h.server.broadcastEvent("import_done", map[string]any{
		"session_id": h.sessionID,
		"progress":   p,
	})
}

func (h sessionHandler) OnError(err error) {
	logger.Error("import session error", "session_id", h.sessionID, "error", err)
	h.server.broadcastEvent("import_error", map[string]any{
		"session_id": h.sessionID,
		"error":      err.Error(),
	})
}

func parseSearchTimeRange(startInput string, endInput string) (time.Time, time.Time, error) {
	start := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
	end := time.Date(9999, 12, 31, 23, 59, 0, 0, time.Local)

	var err error
	if strings.TrimSpace(startInput) != "" {
		start, err = parseMinuteTime(startInput)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	if strings.TrimSpace(endInput) != "" {
		end, err = parseMinuteTime(endInput)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	if end.Before(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("end before start")
	}
	return start, end, nil
}

func parseMinuteTime(value string) (time.Time, error) {
	value = strings.TrimSpace(strings.ReplaceAll(value, "T", " "))
	layouts := []string{
		"2006-01-02 15:04",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if ts, err := time.ParseInLocation(layout, value, time.Local); err == nil {
			return ts.Truncate(time.Minute), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported format: %s", value)
}

func parsePageArgs(pageRaw string, pageSizeRaw string) (int, int) {
	page := 1
	pageSize := 100
	if n, err := strconv.Atoi(strings.TrimSpace(pageRaw)); err == nil && n > 0 {
		page = n
	}
	if n, err := strconv.Atoi(strings.TrimSpace(pageSizeRaw)); err == nil && n > 0 {
		pageSize = n
	}
	if pageSize > 200 {
		pageSize = 200
	}
	return page, pageSize
}

func parseLimitArg(raw string, defaultLimit int, maxLimit int) int {
	limit := defaultLimit
	if n, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && n > 0 {
		limit = n
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	return limit
}

func parseOptionalEndTime(endRaw string) (time.Time, error) {
	if strings.TrimSpace(endRaw) == "" {
		return time.Now().Truncate(time.Minute), nil
	}
	return parseMinuteTime(endRaw)
}

func inferKlineTypeBySymbol(symbol string) string {
	s := strings.ToLower(strings.TrimSpace(symbol))
	if s == "l9" || strings.HasSuffix(s, "l9") {
		return "l9"
	}
	return "contract"
}
