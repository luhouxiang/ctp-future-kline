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

	"ctp-go-demo/internal/bus"
	"ctp-go-demo/internal/calendar"
	"ctp-go-demo/internal/chartlayout"
	"ctp-go-demo/internal/config"
	dbx "ctp-go-demo/internal/db"
	"ctp-go-demo/internal/importer"
	"ctp-go-demo/internal/klinequery"
	"ctp-go-demo/internal/logger"
	"ctp-go-demo/internal/replay"
	"ctp-go-demo/internal/searchindex"
	"ctp-go-demo/internal/strategy"
	"ctp-go-demo/internal/trade"
	"ctp-go-demo/internal/trader"

	"github.com/gorilla/websocket"
)

type Server struct {
	cfg      config.AppConfig
	status   *trader.RuntimeStatusCenter
	runtime  runtimeStarter
	search   *searchindex.Manager
	query    *klinequery.Service
	calendar *calendar.Manager
	replay   *replay.Service
	chart    *chartlayout.Service
	strategy *strategy.Manager
	trade    *trade.Service

	mu        sync.Mutex
	wsWriteMu sync.Mutex
	sessions  map[string]*importer.TDXImportSession
	wsConns   map[*websocket.Conn]struct{}
}

type runtimeStarter interface {
	Start() error
}

func NewServer(cfg config.AppConfig) *Server {
	status := trader.NewRuntimeStatusCenter(time.Duration(cfg.Web.MarketOpenStaleSeconds) * time.Second)
	dsn := dbx.BuildDSN(cfg.DB)
	cfg.CTP.DBDSN = dsn
	search := searchindex.NewManager(dsn, 30*time.Second)
	s := &Server{
		cfg:      cfg,
		status:   status,
		runtime:  trader.NewRuntimeManager(cfg.CTP, status),
		search:   search,
		query:    klinequery.NewService(dsn, search),
		calendar: calendar.NewManager(dsn),
		sessions: make(map[string]*importer.TDXImportSession),
		wsConns:  make(map[*websocket.Conn]struct{}),
	}
	if err := dbx.EnsureDatabase(cfg.DB); err != nil {
		logger.Error("ensure mysql database failed", "error", err)
	} else if db, err := dbx.Open(dsn); err != nil {
		logger.Error("open mysql failed", "error", err)
	} else {
		if err := dbx.EnsureDatabaseAndSchema(cfg.DB, db); err != nil {
			logger.Error("ensure mysql schema failed", "error", err)
		}
		_ = db.Close()
	}
	if cfg.CTP.IsBusEnabled() {
		busPath := strings.TrimSpace(cfg.CTP.BusLogPath)
		if busPath == "" {
			busPath = filepath.Join(cfg.CTP.FlowPath, "bus")
		}
		busLog := bus.NewFileLog(busPath, time.Duration(cfg.CTP.BusFlushMS)*time.Millisecond)
		db, err := dbx.Open(dsn)
		if err != nil {
			logger.Error("open replay dedup db failed", "error", err)
		} else {
			store, err := bus.NewConsumerStore(db)
			if err != nil {
				logger.Error("init replay dedup store failed", "error", err)
			} else {
				s.replay = replay.NewService(busLog, store, cfg.CTP.IsReplayAllowOrderCommandDispatch())
				replaySink, sinkErr := trader.NewReplaySink(cfg.CTP, status)
				if sinkErr != nil {
					logger.Error("init replay trader sink failed", "error", sinkErr)
				} else {
					s.replay.RegisterConsumer("trader.replay_sink", replaySink.ConsumeBusEvent)
					s.replay.RegisterTaskLifecycle("trader.replay_sink", replaySink)
				}
			}
		}
	}
	if db, err := dbx.Open(dsn); err != nil {
		logger.Error("open chart layout db failed", "error", err)
	} else {
		store, err := chartlayout.NewStore(db)
		if err != nil {
			logger.Error("init chart layout store failed", "error", err)
		} else {
			s.chart = chartlayout.NewService(store)
		}
	}
	if cfg.Strategy.IsEnabled() {
		manager, err := strategy.NewManager(cfg.Strategy, dsn)
		if err != nil {
			logger.Error("init strategy manager failed", "error", err)
		} else {
			s.strategy = manager
		}
	}
	if cfg.Trade.IsEnabled() {
		svc, err := trade.NewService(cfg.Trade, cfg.CTP, dsn)
		if err != nil {
			logger.Error("init trade service failed", "error", err)
		} else {
			s.trade = svc
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
	if s.strategy != nil {
		if err := s.strategy.Start(); err != nil {
			logger.Error("start strategy manager failed", "error", err)
		}
		go s.forwardStrategyEvents()
	}
	if s.trade != nil {
		if err := s.trade.Start(); err != nil {
			logger.Error("start trade service failed", "error", err)
		} else {
			go s.forwardTradeEvents()
		}
	}
	logger.Info("web server listening", "addr", s.cfg.Web.ListenAddr)
	return http.ListenAndServe(s.cfg.Web.ListenAddr, mux)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/server/start", s.handleStartRuntime)
	mux.HandleFunc("/api/import/session", s.handleImportSession)
	mux.HandleFunc("/api/import/session/", s.handleImportDecision)
	mux.HandleFunc("/api/kline/search", s.handleKlineSearch)
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

func (s *Server) CurrentStatus() trader.RuntimeSnapshot {
	return s.status.Snapshot(time.Now())
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]any{"status": s.status.Snapshot(time.Now())}
	if s.replay != nil {
		resp["replay"] = s.replay.Status()
	}
	if s.strategy != nil {
		resp["strategy"] = s.strategy.Status()
	}
	if s.trade != nil {
		resp["trade"] = s.trade.Status()
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleStartRuntime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
	dbPath := s.cfg.CTP.DBDSN

	session := importer.NewTDXImportSession(sessionID, dbPath, uploadFiles, sessionHandler{
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
	s.wsConns[conn] = struct{}{}
	s.mu.Unlock()

	_ = conn.WriteJSON(map[string]any{
		"type": "status_update",
		"data": s.statusPayload(),
	})

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.wsConns, conn)
			s.mu.Unlock()
			conn.Close()
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

func (s *Server) handleKlineSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	keyword := strings.TrimSpace(r.URL.Query().Get("keyword"))
	start, end, err := parseSearchTimeRange(r.URL.Query().Get("start"), r.URL.Query().Get("end"))
	if err != nil {
		http.Error(w, "invalid start/end: "+err.Error(), http.StatusBadRequest)
		return
	}
	page, pageSize := parsePageArgs(r.URL.Query().Get("page"), r.URL.Query().Get("page_size"))

	resp, err := s.query.Search(keyword, start, end, page, pageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
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
	resp, err := s.query.BarsByEnd(symbol, kind, variety, timeframe, end, limit)
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
	resp, err := s.query.ListContracts(page, pageSize)
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
	if s.chart == nil {
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
		layout, ok, err := s.chart.Get(owner, symbol, kind, variety, timeframe)
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
		out, err := s.chart.Put(req)
		if err != nil {
			// Self-heal for legacy schema: retry once after ensuring latest MySQL schema.
			if strings.Contains(err.Error(), "Unknown column 'owner'") || strings.Contains(err.Error(), "Unknown column `owner`") {
				logger.Info("api chart layout put schema mismatch detected, attempting schema ensure and retry")
				dsn := dbx.BuildDSN(s.cfg.DB)
				if db, openErr := dbx.Open(dsn); openErr != nil {
					logger.Error("api chart layout put schema self-heal open db failed", "error", openErr)
				} else {
					if ensureErr := dbx.EnsureDatabaseAndSchema(s.cfg.DB, db); ensureErr != nil {
						logger.Error("api chart layout put schema self-heal ensure failed", "error", ensureErr)
					} else {
						logger.Info("api chart layout put schema self-heal ensure success")
						out, err = s.chart.Put(req)
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
	if s.chart == nil {
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
	out, err := s.chart.UpsertDrawing(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleChartDrawingsByID(w http.ResponseWriter, r *http.Request) {
	if s.chart == nil {
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
	ok, err := s.chart.DeleteDrawing(owner, symbol, kind, variety, timeframe, id)
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
	if s.trade == nil {
		http.Error(w, "trade service unavailable", http.StatusNotFound)
		return nil
	}
	return s.trade
}

func (s *Server) handleTradeStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	svc := s.requireTrade(w)
	if svc == nil {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": svc.Status()})
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

func (s *Server) forwardTradeEvents() {
	if s.trade == nil {
		return
	}
	ch, cancel := s.trade.Subscribe()
	defer cancel()
	for ev := range ch {
		s.broadcastEvent(ev.Type, ev.Data)
	}
}

func (s *Server) broadcastStatusTicker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.broadcastEvent("status_update", s.statusPayload())
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
	if s.trade != nil {
		out["trade"] = s.trade.Status()
	}
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
		if err := conn.WriteJSON(payload); err != nil {
			s.mu.Lock()
			delete(s.wsConns, conn)
			s.mu.Unlock()
			conn.Close()
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
	server    *Server
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
	if h.server.search != nil {
		h.server.search.Invalidate()
		go func(sessionID string) {
			start := time.Now()
			logger.Info("kline search index refresh begin", "session_id", sessionID)
			if err := h.server.search.EnsureFresh(); err != nil {
				logger.Error("kline search index refresh failed", "session_id", sessionID, "error", err)
				return
			}
			logger.Info("kline search index refresh done", "session_id", sessionID, "elapsed_ms", time.Since(start).Milliseconds())
		}(h.sessionID)
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
