// service.go 负责装配实时行情链路。
// 它先通过查询前置完成认证、登录和合约查询，再启动 MD 订阅链路，
// 把 mdSpi、marketDataRuntime、klineStore、L9 计算、bus、CSV 录制等模块串成完整处理管道。
package quotes

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/strategy"

	ctp "github.com/kkqy/ctp-go"
)

const minQueryCallbacksWait = 15 * time.Second

type Service struct {
	// cfg 保存行情主链路运行所需的 CTP 配置。
	cfg config.CTPConfig
	// bus 延迟初始化 bus 文件日志，用于把 tick 和 bar 旁路输出给 replay 等消费者。
	bus struct {
		// once 确保 bus 只初始化一次。
		once sync.Once
		// log 是已创建的 bus 文件日志实例。
		log *bus.FileLog
		// err 保存初始化 bus 时的错误。
		err error
	}
}

type instrumentInfo struct {
	// ID 是合约代码，例如 ag2605。
	ID string
	// ExchangeID 是交易所代码，例如 SHFE。
	ExchangeID string
	// ProductID 是品种代码，例如 ag。
	ProductID string
	// ProductClass 是 CTP 返回的产品类别枚举值。
	ProductClass byte
}

var domesticFuturesExchanges = map[string]struct{}{
	"CFFEX": {},
	"CZCE":  {},
	"DCE":   {},
	"GFEX":  {},
	"INE":   {},
	"SHFE":  {},
}

func NewService(cfg config.CTPConfig) *Service {
	if count, err := DefaultProductExchangeCache().EnsureLoadedFromDSN(cfg.SharedMetaDSN); err != nil {
		logger.Error("load product exchange cache on service init failed", "error", err)
	} else {
		logger.Info("product exchange cache ready", "source", "quotes_service_init", "product_exchange_count", count)
	}
	return &Service{cfg: cfg}
}

// Run 执行一次完整的实时链路：
// 1. 查询可订阅合约
// 2. 订阅行情并接收实时 tick
// 3. 在 mdSpi 中聚合成 1m
// 4. 继续触发 mm 与 L9 计算
func (s *Service) Run() error {
	logger.Info("service start")
	if err := os.MkdirAll(s.cfg.FlowPath, 0o755); err != nil {
		logger.Error("create flow directory failed", "flow_path", s.cfg.FlowPath, "error", err)
		return fmt.Errorf("create flow directory failed: %w", err)
	}
	logger.Info("flow directory ready", "flow_path", s.cfg.FlowPath)

	instruments, err := s.runQueryStage(nil)
	if err != nil {
		logger.Error("query stage failed", "error", err)
		return err
	}
	logger.Info("query stage completed", "instrument_count", len(instruments))

	if err := s.runMarketDataOnce(instruments, nil); err != nil {
		logger.Error("market data stage failed", "error", err)
		return err
	}

	logger.Info("service end")
	return nil
}

// RunContinuous 与 Run 类似，但会把运行态指标持续暴露给 status。
func (s *Service) RunContinuous(status *RuntimeStatusCenter) error {
	logger.Info("service continuous start")
	if err := os.MkdirAll(s.cfg.FlowPath, 0o755); err != nil {
		return fmt.Errorf("create flow directory failed: %w", err)
	}

	instruments, err := s.runQueryStage(status)
	if err != nil {
		return err
	}
	return s.runMarketDataContinuous(instruments, status)
}

func (s *Service) runQueryStage(status *RuntimeStatusCenter) ([]instrumentInfo, error) {
	logger.Info("query stage start")
	var spi *querySpi
	if status != nil {
		spi = newQuerySpiWithStatus(status)
	} else {
		spi = newQuerySpi()
	}

	api := ctp.CThostFtdcTraderApiCreateFtdcTraderApi(s.cfg.FlowPath)
	defer api.Release()

	api.RegisterSpi(ctp.NewDirectorCThostFtdcTraderSpi(spi))
	api.RegisterFront(s.cfg.TraderFrontAddr)
	api.SubscribePrivateTopic(ctp.THOST_TERT_RESTART)
	api.SubscribePublicTopic(ctp.THOST_TERT_RESTART)
	api.Init()
	logger.Info("query api init done")

	time.Sleep(time.Duration(s.cfg.ConnectWaitSeconds) * time.Second)

	if err := s.authenticate(api, spi.nextReqID()); err != nil {
		logger.Error("authenticate request failed", "error", err)
		return nil, err
	}
	logger.Info("authenticate request sent")
	time.Sleep(time.Duration(s.cfg.AuthenticateWaitSeconds) * time.Second)

	if err := s.login(api, spi.nextReqID()); err != nil {
		logger.Error("user login request failed", "error", err)
		return nil, err
	}
	logger.Info("user login request sent")
	time.Sleep(time.Duration(s.cfg.LoginWaitSeconds) * time.Second)

	metaDB, err := openSharedMetaDB(s.cfg)
	if err != nil {
		logger.Error("open shared meta db failed", "error", err)
		return nil, err
	}
	defer metaDB.Close()
	repo := NewInstrumentCatalogRepo(metaDB)

	tradingDay := spi.getTradingDay()
	if syncLog, ok, err := repo.LatestSyncLog(tradingDay); err != nil {
		logger.Error("load instrument sync log failed", "trading_day", tradingDay, "error", err)
	} else if ok {
		instruments, loadErr := repo.ListInstrumentInfosByTradingDay(tradingDay)
		if loadErr != nil {
			logger.Error("load instrument catalog failed", "trading_day", tradingDay, "error", loadErr)
		} else if len(instruments) > 0 {
			logger.Info(
				"instrument catalog already synced for trading day",
				"trading_day", tradingDay,
				"instrument_count", syncLog.InstrumentCount,
				"updated_at", syncLog.UpdatedAt.Format("2006-01-02 15:04:05"),
			)
			s.startCommissionCatalogSyncAsync(tradingDay, instruments)
			return instruments, nil
		}
		logger.Warn(
			"instrument sync log exists but catalog rows missing, falling back to ctp query",
			"trading_day", tradingDay,
			"instrument_count", syncLog.InstrumentCount,
			"updated_at", syncLog.UpdatedAt.Format("2006-01-02 15:04:05"),
		)
	}

	if err := s.queryInstrument(api, spi.nextReqID()); err != nil {
		logger.Error("query instrument request failed", "error", err)
		return nil, err
	}
	logger.Info("query instrument request sent")

	waitTimeout := s.queryCallbacksWaitTimeout()
	var instruments []instrumentInfo
	select {
	case <-spi.queryFinished:
		instruments = dedupeInstrumentInfos(spi.instrumentInfos())
		logger.Info("query instrument callbacks finished")
	case <-time.After(waitTimeout):
		return nil, fmt.Errorf("wait query instrument callbacks timeout after %s", waitTimeout)
	}

	updatedProductCount, err := repo.SyncTradingDay(tradingDay, spi.instrumentSnapshots(), time.Now())
	if err != nil {
		logger.Error("sync instrument catalog failed", "trading_day", tradingDay, "error", err)
	} else if updatedProductCount > 0 {
		count, loadErr := DefaultProductExchangeCache().LoadFromDSN(s.cfg.SharedMetaDSN)
		if loadErr != nil {
			logger.Error(
				"refresh product exchange cache after sync failed",
				"trading_day", tradingDay,
				"updated_product_count", updatedProductCount,
				"error", loadErr,
			)
		} else {
			logger.Info(
				"product exchange cache refreshed",
				"trading_day", tradingDay,
				"updated_product_count", updatedProductCount,
				"product_exchange_count", count,
			)
		}
	}
	s.startCommissionCatalogSyncAsync(tradingDay, instruments)

	instrumentIDs := make([]string, 0, len(instruments))
	for _, item := range instruments {
		instrumentIDs = append(instrumentIDs, item.ID)
	}
	logger.Info(
		"query result summary",
		"trading_day", tradingDay,
		"instrument_count", len(instrumentIDs),
		"instruments", instrumentIDs,
	)
	return instruments, nil
}

// startCommissionCatalogSyncAsync 在后台独立会话中补齐手续费目录，避免阻塞 MD 订阅启动。
func (s *Service) startCommissionCatalogSyncAsync(tradingDay string, instruments []instrumentInfo) {
	day := strings.TrimSpace(tradingDay)
	if day == "" || len(instruments) == 0 {
		return
	}
	items := append([]instrumentInfo(nil), instruments...)
	go func() {
		if err := s.syncCommissionCatalogStandalone(day, items); err != nil {
			logger.Error("commission catalog async sync failed", "trading_day", day, "error", err)
		}
	}()
}

func (s *Service) syncCommissionCatalogStandalone(tradingDay string, instruments []instrumentInfo) error {
	flowPath := filepath.Join(s.cfg.FlowPath, "commission_sync")
	if err := os.MkdirAll(flowPath, 0o755); err != nil {
		return fmt.Errorf("create commission sync flow directory failed: %w", err)
	}

	spi := newQuerySpi()
	api := ctp.CThostFtdcTraderApiCreateFtdcTraderApi(flowPath)
	defer api.Release()

	api.RegisterSpi(ctp.NewDirectorCThostFtdcTraderSpi(spi))
	api.RegisterFront(s.cfg.TraderFrontAddr)
	api.SubscribePrivateTopic(ctp.THOST_TERT_RESTART)
	api.SubscribePublicTopic(ctp.THOST_TERT_RESTART)
	api.Init()

	time.Sleep(time.Duration(s.cfg.ConnectWaitSeconds) * time.Second)
	if err := s.authenticate(api, spi.nextReqID()); err != nil {
		return fmt.Errorf("commission sync authenticate failed: %w", err)
	}
	time.Sleep(time.Duration(s.cfg.AuthenticateWaitSeconds) * time.Second)
	if err := s.login(api, spi.nextReqID()); err != nil {
		return fmt.Errorf("commission sync login failed: %w", err)
	}
	time.Sleep(time.Duration(s.cfg.LoginWaitSeconds) * time.Second)

	metaDB, err := openSharedMetaDB(s.cfg)
	if err != nil {
		return fmt.Errorf("commission sync open shared meta db failed: %w", err)
	}
	defer metaDB.Close()

	repo := NewInstrumentCatalogRepo(metaDB)
	if err := s.syncCommissionCatalog(api, spi, repo, tradingDay, instruments); err != nil {
		return fmt.Errorf("commission sync failed: %w", err)
	}
	logger.Info("commission catalog async sync completed", "trading_day", tradingDay, "instrument_count", len(instruments))
	return nil
}

func (s *Service) syncCommissionCatalog(api ctp.CThostFtdcTraderApi, spi *querySpi, repo *InstrumentCatalogRepo, tradingDay string, instruments []instrumentInfo) error {
	if api == nil || spi == nil || repo == nil {
		return nil
	}
	start := time.Now()
	spi.resetCommissionRateSnapshots()
	targets := buildCommissionTargets(instruments)
	if len(targets) == 0 {
		logger.Warn("commission catalog sync skipped: no valid instrument target", "trading_day", tradingDay)
		return nil
	}
	queryMode := "per_instrument"
	queried := 0
	failures := 0
	for idx, item := range targets {
		if err := s.queryInstrumentCommissionRateWait(api, spi, item.ID, item.ExchangeID, 12*time.Second); err != nil {
			failures++
			logger.Error(
				"commission catalog query failed",
				"trading_day", strings.TrimSpace(tradingDay),
				"instrument", strings.TrimSpace(item.ID),
				"exchange", strings.TrimSpace(item.ExchangeID),
				"error", err,
			)
		} else {
			queried++
		}
		// Respect CTP query flow-control: at most 1 query/second.
		if idx < len(targets)-1 {
			time.Sleep(time.Second)
		}
	}
	snapshots := spi.commissionRateSnapshots()
	written, err := repo.SyncCommissionRates(tradingDay, snapshots, time.Now())
	if err != nil {
		return err
	}
	exchanges := make(map[string]int, 8)
	unknownExchange := 0
	for _, item := range snapshots {
		ex := strings.TrimSpace(item.ExchangeID)
		if ex == "" {
			unknownExchange++
			continue
		}
		exchanges[ex]++
	}
	logger.Info(
		"commission catalog sync summary",
		"trading_day", tradingDay,
		"query_mode", queryMode,
		"queried_instruments", queried,
		"query_failures", failures,
		"target_instruments", len(targets),
		"snapshot_rows", len(snapshots),
		"written_rows", written,
		"exchange_distribution", exchanges,
		"unknown_exchange_rows", unknownExchange,
		"elapsed_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

func (s *Service) queryInstrumentCommissionRateWait(api ctp.CThostFtdcTraderApi, spi *querySpi, instrumentID string, exchangeID string, timeout time.Duration) error {
	reqID := spi.nextReqID()
	waitCh := spi.registerCommissionWaiter(reqID)
	if err := s.queryInstrumentCommissionRate(api, reqID, instrumentID, exchangeID); err != nil {
		return err
	}
	select {
	case result := <-waitCh:
		if result.ErrID != 0 {
			return fmt.Errorf(
				"query instrument commission rate failed, req_id=%d, instrument=%s, exchange=%s, error_id=%d, error_msg=%s",
				reqID,
				instrumentID,
				exchangeID,
				result.ErrID,
				result.ErrMsg,
			)
		}
		if result.Rows == 0 {
			logger.Warn(
				"query instrument commission rate returned no rows",
				"req_id", reqID,
				"instrument", instrumentID,
				"exchange", exchangeID,
				"error_id", result.ErrID,
				"error_msg", result.ErrMsg,
			)
		}
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("query instrument commission rate timeout, req_id=%d, instrument=%s, exchange=%s", reqID, instrumentID, exchangeID)
	}
}

func buildCommissionTargets(instruments []instrumentInfo) []instrumentInfo {
	if len(instruments) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(instruments))
	out := make([]instrumentInfo, 0, len(instruments))
	for _, item := range instruments {
		id := strings.TrimSpace(item.ID)
		exchange := strings.TrimSpace(item.ExchangeID)
		if id == "" || exchange == "" {
			continue
		}
		key := strings.ToLower(id) + "|" + exchange
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, instrumentInfo{ID: id, ExchangeID: exchange})
	}
	return out
}

func (s *Service) queryCallbacksWaitTimeout() time.Duration {
	wait := time.Duration(s.cfg.ConnectWaitSeconds+s.cfg.AuthenticateWaitSeconds+s.cfg.LoginWaitSeconds) * time.Second
	if wait < minQueryCallbacksWait {
		wait = minQueryCallbacksWait
	}
	return wait + 10*time.Second
}

func (s *Service) runMarketDataOnce(queriedInstruments []instrumentInfo, status *RuntimeStatusCenter) error {
	spi, _, _, err := s.initMarketData(queriedInstruments, status)
	if err != nil {
		return err
	}

	logger.Info("md subscription started", "receive_seconds", s.cfg.MdReceiveSeconds)
	time.Sleep(time.Duration(s.cfg.MdReceiveSeconds) * time.Second)
	if err := spi.Flush(); err != nil {
		logger.Error("flush minute bars failed", "error", err)
		return err
	}
	logger.Info("market data stage finished")
	return nil
}

func (s *Service) runMarketDataContinuous(queriedInstruments []instrumentInfo, status *RuntimeStatusCenter) error {
	_, _, session, err := s.initMarketData(queriedInstruments, status)
	if err != nil {
		return err
	}
	if session != nil {
		session.Start()
	}
	if status != nil {
		status.SetRunning()
	}
	select {}
}

// initMarketData 组装实时行情处理链路。
//
// 这里会把几条核心子链路接在一起：
// 1. klineStore: 负责 1m/L9 落库
// 2. l9AsyncCalculator: 负责异步生成 L9
// 3. tickCSVRecorder: 负责把实时 tick 录成可回放 CSV
// 4. busLog: 负责把 tick/bar 旁路写到事件总线
// 5. mdSpi: 负责把实时 tick 聚合成 1m，并驱动后续 mm/L9
func (s *Service) initMarketData(queriedInstruments []instrumentInfo, status *RuntimeStatusCenter) (*mdSpi, *klineStore, *mdSession, error) {
	logger.Info("market data stage start")
	dbPath, err := resolveStoreDSN(s.cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	store, err := newKlineStore(dbPath)
	if err != nil {
		logger.Error("open kline store failed", "db_path", dbPath, "error", err)
		return nil, nil, nil, err
	}
	logger.Info("kline store ready", "db_path", dbPath)
	metaDB, err := openSharedMetaDB(s.cfg)
	if err != nil {
		_ = store.Close()
		return nil, nil, nil, err
	}

	subscribeTargets := selectSubscribeTargets(queriedInstruments, s.cfg.SubscribeInstruments)
	if len(subscribeTargets) == 0 {
		_ = metaDB.Close()
		_ = store.Close()
		logger.Error("no instruments to subscribe")
		return nil, nil, nil, fmt.Errorf("no instruments to subscribe")
	}
	expectedByVariety := buildExpectedVarietyInstruments(queriedInstruments, subscribeTargets)
	var l9Calc *l9AsyncCalculator
	generation := GetKlineGenerationSettings()
	if s.cfg.IsL9AsyncEnabled() && generation.AnyEnabled("l9") {
		l9Calc = newL9AsyncCalculator(store, metaDB, status, true, 1, expectedByVariety)
	}

	var session *mdSession
	busLog, _ := s.getBusLog()
	sideEffects := newMarketDataSideEffects(status,
		func(t tickEvent) {
			PublishRealtimeChartTick(t)
			strategy.PublishRealtimeTick(strategy.TickEvent{
				InstrumentID:    t.InstrumentID,
				ExchangeID:      t.ExchangeID,
				ActionDay:       t.ActionDay,
				TradingDay:      t.TradingDay,
				UpdateTime:      t.UpdateTime,
				UpdateMillisec:  t.UpdateMillisec,
				ReceivedAt:      t.ReceivedAt,
				LastPrice:       t.LastPrice,
				Volume:          t.Volume,
				OpenInterest:    t.OpenInterest,
				SettlementPrice: t.SettlementPrice,
				BidPrice1:       t.BidPrice1,
				AskPrice1:       t.AskPrice1,
			})
		},
		func(bar minuteBar) {
			strategy.PublishRealtimeBar(strategy.BarEvent{
				Variety:         bar.Variety,
				InstrumentID:    bar.InstrumentID,
				Exchange:        bar.Exchange,
				DataTime:        bar.MinuteTime,
				AdjustedTime:    bar.AdjustedTime,
				Period:          bar.Period,
				Open:            bar.Open,
				High:            bar.High,
				Low:             bar.Low,
				Close:           bar.Close,
				Volume:          bar.Volume,
				OpenInterest:    bar.OpenInterest,
				SettlementPrice: bar.SettlementPrice,
			})
			if busLog == nil {
				return
			}
			payload, err := json.Marshal(bar)
			if err != nil {
				return
			}
			_, _ = busLog.Append(bus.BusEvent{
				EventID:    bus.NewEventID(),
				Topic:      bus.TopicBar,
				Source:     "quotes.bar",
				OccurredAt: bar.MinuteTime,
				Payload:    payload,
			})
		},
	)
	options := mdSpiOptions{
		tickDedupWindow:    time.Duration(s.cfg.TickDedupWindowSeconds) * time.Second,
		driftThreshold:     time.Duration(s.cfg.DriftThresholdSeconds) * time.Second,
		driftResumeTicks:   s.cfg.DriftResumeTicks,
		enableMultiMinute:  generation.AnyHigherEnabled("contract"),
		dbWriterCount:      s.cfg.DBWriterCount,
		dbFlushBatch:       s.cfg.DBFlushBatch,
		dbFlushInterval:    time.Duration(s.cfg.DBFlushIntervalMS) * time.Millisecond,
		mmDeferredInterval: time.Duration(s.cfg.MMDeferredIntervalMS) * time.Millisecond,
		mmDeferredBatch:    s.cfg.MMDeferredBatch,
		flowPath:           s.cfg.FlowPath,
		generation:         generation,
		onTick:             sideEffects.PublishTick,
		onBar:              sideEffects.PublishBar,
		onPartialBar: func(bar minuteBar) {
			PublishChartPartialBar(bar, false)
		},
		onPersistTask: func(task persistTask) {
			PublishChartFinalBar(task.Bar, task.Replay)
		},
	}

	var spi *mdSpi
	if status != nil {
		spi = newMdSpiWithStatusAndOptions(store, metaDB, l9Calc, status, options)
	} else {
		spi = newMdSpiWithOptions(store, metaDB, l9Calc, options)
	}
	api := ctp.CThostFtdcMdApiCreateFtdcMdApi(s.cfg.FlowPath)

	api.RegisterSpi(ctp.NewDirectorCThostFtdcMdSpi(spi))
	api.RegisterFront(s.cfg.MdFrontAddr)
	api.Init()
	logger.Info("md api init done")

	time.Sleep(time.Duration(s.cfg.MdConnectWaitSeconds) * time.Second)

	if err := s.loginMd(api, 10001); err != nil {
		_ = metaDB.Close()
		_ = store.Close()
		logger.Error("md login request failed", "error", err)
		return nil, nil, nil, err
	}
	logger.Info("md login request sent")
	time.Sleep(time.Duration(s.cfg.MdLoginWaitSeconds) * time.Second)

	logger.Info("subscribe market data", "instrument_count", len(subscribeTargets))
	logger.Info("subscribe market data targets", "instrument_count", len(subscribeTargets), "instruments", subscribeTargets)
	for _, instrumentID := range subscribeTargets {
		logger.Info("subscribe instrument target", "instrument_id", instrumentID)
	}
	subscribe := func() error {
		if ret := api.SubscribeMarketData(subscribeTargets); ret != 0 {
			return fmt.Errorf("SubscribeMarketData failed, code: %d", ret)
		}
		return nil
	}
	if err := subscribe(); err != nil {
		_ = metaDB.Close()
		_ = store.Close()
		logger.Error("SubscribeMarketData failed", "error", err)
		return nil, nil, nil, err
	}
	if status != nil {
		status.MarkSubscribed(len(subscribeTargets))
	}

	if status != nil {
		session = newMDSession(s.cfg, status, subscribeTargets, mdSessionOps{
			login: func() error {
				return s.loginMd(api, 10001)
			},
			subscribe: subscribe,
		})
		spi.onDisconnected = session.NotifyDisconnected
	}
	return spi, store, session, nil
}

func (s *Service) getBusLog() (*bus.FileLog, error) {
	s.bus.once.Do(func() {
		if !s.cfg.IsBusEnabled() {
			return
		}
		logPath := strings.TrimSpace(s.cfg.BusLogPath)
		if logPath == "" {
			logPath = filepath.Join(s.cfg.FlowPath, "bus")
		}
		s.bus.log = bus.NewFileLog(logPath, time.Duration(s.cfg.BusFlushMS)*time.Millisecond)
	})
	return s.bus.log, s.bus.err
}

func (s *Service) authenticate(api ctp.CThostFtdcTraderApi, reqID int) error {
	field := ctp.NewCThostFtdcReqAuthenticateField()
	defer ctp.DeleteCThostFtdcReqAuthenticateField(field)

	field.SetBrokerID(s.cfg.BrokerID)
	field.SetUserID(s.cfg.UserID)
	// field.SetUserProductInfo(s.cfg.UserProductInfo)
	field.SetAuthCode(s.cfg.AuthCode)
	field.SetAppID(s.cfg.AppID)
	logger.Info(
		"trade ctp request",
		"api", "query",
		"method", "ReqAuthenticate",
		"req_id", reqID,
		"broker_id", s.cfg.BrokerID,
		"user_id", s.cfg.UserID,
		"user_product_info", s.cfg.UserProductInfo,
		"app_id", s.cfg.AppID,
		"auth_code", s.cfg.AuthCode,
	)

	if ret := api.ReqAuthenticate(field, reqID); ret != 0 {
		return fmt.Errorf("ReqAuthenticate failed, code: %d", ret)
	}
	return nil
}

func (s *Service) login(api ctp.CThostFtdcTraderApi, reqID int) error {
	field := ctp.NewCThostFtdcReqUserLoginField()
	defer ctp.DeleteCThostFtdcReqUserLoginField(field)

	field.SetBrokerID(s.cfg.BrokerID)
	field.SetUserID(s.cfg.UserID)
	field.SetPassword(s.cfg.Password)
	field.SetProtocolInfo(s.cfg.UserProductInfo)
	logger.Info(
		"trade ctp request",
		"api", "query",
		"method", "ReqUserLogin",
		"req_id", reqID,
		"broker_id", s.cfg.BrokerID,
		"user_id", s.cfg.UserID,
		"password", s.cfg.Password,
		"ProtocolInfo", s.cfg.UserProductInfo,
	)

	if ret := api.ReqUserLogin(field, reqID); ret != 0 {
		return fmt.Errorf("ReqUserLogin failed, code: %d", ret)
	}
	return nil
}

func (s *Service) loginMd(api ctp.CThostFtdcMdApi, reqID int) error {
	field := ctp.NewCThostFtdcReqUserLoginField()
	defer ctp.DeleteCThostFtdcReqUserLoginField(field)

	field.SetBrokerID(s.cfg.BrokerID)
	field.SetUserID(s.cfg.UserID)
	field.SetPassword(s.cfg.Password)
	logger.Info(
		"ctp request",
		"api", "md",
		"method", "ReqUserLogin",
		"req_id", reqID,
		"broker_id", s.cfg.BrokerID,
		"user_id", s.cfg.UserID,
		"password", maskSecret(s.cfg.Password),
	)

	if ret := api.ReqUserLogin(field, reqID); ret != 0 {
		return fmt.Errorf("Md ReqUserLogin failed, code: %d", ret)
	}
	return nil
}

func (s *Service) queryInstrument(api ctp.CThostFtdcTraderApi, reqID int) error {
	field := ctp.NewCThostFtdcQryInstrumentField()
	defer ctp.DeleteCThostFtdcQryInstrumentField(field)
	logger.Info(
		"ctp request",
		"api", "query",
		"method", "ReqQryInstrument",
		"req_id", reqID,
		"broker_id", s.cfg.BrokerID,
		"user_id", s.cfg.UserID,
	)

	if ret := api.ReqQryInstrument(field, reqID); ret != 0 {
		return fmt.Errorf("ReqQryInstrument failed, code: %d", ret)
	}
	return nil
}

func (s *Service) queryInstrumentCommissionRate(api ctp.CThostFtdcTraderApi, reqID int, instrumentID string, exchangeID string) error {
	field := ctp.NewCThostFtdcQryInstrumentCommissionRateField()
	defer ctp.DeleteCThostFtdcQryInstrumentCommissionRateField(field)
	field.SetBrokerID(s.cfg.BrokerID)
	field.SetInvestorID(s.cfg.UserID)
	field.SetInstrumentID(strings.TrimSpace(instrumentID))
	field.SetExchangeID(strings.TrimSpace(exchangeID))
	if ret := api.ReqQryInstrumentCommissionRate(field, reqID); ret != 0 {
		return fmt.Errorf("ReqQryInstrumentCommissionRate failed, code: %d, instrument=%s", ret, instrumentID)
	}
	return nil
}

func maskSecret(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if len(s) <= 2 {
		return "**"
	}
	return s[:1] + strings.Repeat("*", len(s)-2) + s[len(s)-1:]
}

func dedupeInstruments(items []string) []string {
	if len(items) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func dedupeInstrumentInfos(items []instrumentInfo) []instrumentInfo {
	if len(items) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(items))
	out := make([]instrumentInfo, 0, len(items))
	for _, item := range items {
		if item.ID == "" {
			continue
		}
		if _, ok := seen[item.ID]; ok {
			continue
		}
		seen[item.ID] = struct{}{}
		out = append(out, item)
	}
	return out
}

func selectSubscribeTargets(queriedInstruments []instrumentInfo, configuredVarieties []string) []string {
	varietyFilter := toVarietyFilter(configuredVarieties)
	filterEnabled := len(varietyFilter) > 0
	if filterEnabled {
		logger.Info("using configured subscribe varieties", "variety_count", len(varietyFilter))
	} else {
		logger.Info("subscribe varieties empty, selecting all domestic futures")
	}

	targets := make([]string, 0, len(queriedInstruments))
	for _, item := range queriedInstruments {
		if item.ID == "" {
			continue
		}
		if !isDomesticFuturesExchange(item.ExchangeID) {
			continue
		}
		if item.ProductClass != ctp.THOST_FTDC_PC_Futures {
			continue
		}

		variety := normalizeVariety(item.ProductID)
		if variety == "" {
			variety = normalizeVariety(item.ID)
		}
		if filterEnabled {
			if _, ok := varietyFilter[variety]; !ok {
				continue
			}
		}
		targets = append(targets, item.ID)
	}
	return dedupeInstruments(targets)
}

func isDomesticFuturesExchange(exchangeID string) bool {
	_, ok := domesticFuturesExchanges[strings.ToUpper(strings.TrimSpace(exchangeID))]
	return ok
}

func toVarietyFilter(values []string) map[string]struct{} {
	filter := make(map[string]struct{}, len(values))
	for _, value := range values {
		v := normalizeVariety(value)
		if v == "" {
			continue
		}
		filter[v] = struct{}{}
	}
	return filter
}

func normalizeVariety(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	end := 0
	for _, ch := range value {
		if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return ""
	}
	return strings.ToLower(value[:end])
}

func buildExpectedVarietyInstruments(queriedInstruments []instrumentInfo, subscribeTargets []string) map[string][]string {
	if len(queriedInstruments) == 0 || len(subscribeTargets) == 0 {
		return nil
	}

	targetSet := make(map[string]struct{}, len(subscribeTargets))
	for _, instrumentID := range subscribeTargets {
		if instrumentID == "" {
			continue
		}
		targetSet[instrumentID] = struct{}{}
	}

	grouped := make(map[string][]string)
	for _, item := range queriedInstruments {
		if item.ID == "" {
			continue
		}
		if _, ok := targetSet[item.ID]; !ok {
			continue
		}
		variety := normalizeVariety(item.ProductID)
		if variety == "" {
			variety = normalizeVariety(item.ID)
		}
		if variety == "" {
			continue
		}
		grouped[variety] = append(grouped[variety], item.ID)
	}
	for variety, ids := range grouped {
		grouped[variety] = dedupeInstruments(ids)
	}
	return grouped
}
