package trader

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"ctp-go-demo/internal/bus"
	"ctp-go-demo/internal/config"
	"ctp-go-demo/internal/logger"

	ctp "github.com/kkqy/ctp-go"
)

const minQueryCallbacksWait = 15 * time.Second

type Service struct {
	cfg config.CTPConfig
	bus struct {
		once sync.Once
		log  *bus.FileLog
		err  error
	}
}

type instrumentInfo struct {
	ID           string
	ExchangeID   string
	ProductID    string
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
	return &Service{cfg: cfg}
}

func (s *Service) Run() error {
	logger.Info("service start")
	if err := os.MkdirAll(s.cfg.FlowPath, 0o755); err != nil {
		logger.Error("create flow directory failed", "flow_path", s.cfg.FlowPath, "error", err)
		return fmt.Errorf("create flow directory failed: %w", err)
	}
	logger.Info("flow directory ready", "flow_path", s.cfg.FlowPath)

	instruments, err := s.runTraderQuery(nil)
	if err != nil {
		logger.Error("trader query stage failed", "error", err)
		return err
	}
	logger.Info("trader query stage completed", "instrument_count", len(instruments))

	if err := s.runMarketDataOnce(instruments, nil); err != nil {
		logger.Error("market data stage failed", "error", err)
		return err
	}

	logger.Info("service end")
	return nil
}

func (s *Service) RunContinuous(status *RuntimeStatusCenter) error {
	logger.Info("service continuous start")
	if err := os.MkdirAll(s.cfg.FlowPath, 0o755); err != nil {
		return fmt.Errorf("create flow directory failed: %w", err)
	}

	instruments, err := s.runTraderQuery(status)
	if err != nil {
		return err
	}
	return s.runMarketDataContinuous(instruments, status)
}

func (s *Service) runTraderQuery(status *RuntimeStatusCenter) ([]instrumentInfo, error) {
	logger.Info("trader query stage start")
	var spi *traderSpi
	if status != nil {
		spi = newTraderSpiWithStatus(status)
	} else {
		spi = newTraderSpi()
	}

	api := ctp.CThostFtdcTraderApiCreateFtdcTraderApi(s.cfg.FlowPath)
	defer api.Release()

	api.RegisterSpi(ctp.NewDirectorCThostFtdcTraderSpi(spi))
	api.RegisterFront(s.cfg.TraderFrontAddr)
	api.SubscribePrivateTopic(ctp.THOST_TERT_RESTART)
	api.SubscribePublicTopic(ctp.THOST_TERT_RESTART)
	api.Init()
	logger.Info("trader api init done")

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

	if err := s.queryInstrument(api, spi.nextReqID()); err != nil {
		logger.Error("query instrument request failed", "error", err)
		return nil, err
	}
	logger.Info("query instrument request sent")

	instrumentResultCh := make(chan []instrumentInfo, 1)
	go func() {
		<-spi.queryFinished
		instrumentResultCh <- dedupeInstrumentInfos(spi.instrumentInfos())
	}()

	waitTimeout := s.queryCallbacksWaitTimeout()
	var instruments []instrumentInfo
	select {
	case instruments = <-instrumentResultCh:
		logger.Info("query instrument callbacks finished")
	case <-time.After(waitTimeout):
		return nil, fmt.Errorf("wait query instrument callbacks timeout after %s", waitTimeout)
	}

	instrumentIDs := make([]string, 0, len(instruments))
	for _, item := range instruments {
		instrumentIDs = append(instrumentIDs, item.ID)
	}
	logger.Info(
		"trader query result summary",
		"trading_day", spi.getTradingDay(),
		"instrument_count", len(instrumentIDs),
		"instruments", instrumentIDs,
	)
	return instruments, nil
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

func (s *Service) initMarketData(queriedInstruments []instrumentInfo, status *RuntimeStatusCenter) (*mdSpi, *klineStore, *mdSession, error) {
	logger.Info("market data stage start")
	dbPath := strings.TrimSpace(s.cfg.DBDSN)
	if dbPath == "" {
		dbPath = filepath.Join(s.cfg.FlowPath, "future_kline.db")
	}
	store, err := newKlineStore(dbPath)
	if err != nil {
		logger.Error("open kline store failed", "db_path", dbPath, "error", err)
		return nil, nil, nil, err
	}
	logger.Info("kline store ready", "db_path", dbPath)

	subscribeTargets := selectSubscribeTargets(queriedInstruments, s.cfg.SubscribeInstruments)
	if len(subscribeTargets) == 0 {
		_ = store.Close()
		logger.Error("no instruments to subscribe")
		return nil, nil, nil, fmt.Errorf("no instruments to subscribe")
	}
	expectedByVariety := buildExpectedVarietyInstruments(queriedInstruments, subscribeTargets)
	l9Calc := newL9AsyncCalculator(store, s.cfg.IsL9AsyncEnabled(), 1, expectedByVariety)

	var session *mdSession
	busLog, _ := s.getBusLog()
	options := mdSpiOptions{
		tickDedupWindow:  time.Duration(s.cfg.TickDedupWindowSeconds) * time.Second,
		driftThreshold:   time.Duration(s.cfg.DriftThresholdSeconds) * time.Second,
		driftResumeTicks: s.cfg.DriftResumeTicks,
		onTick: func(t tickEvent) {
			if busLog == nil {
				return
			}
			payload, err := json.Marshal(t)
			if err != nil {
				return
			}
			_, _ = busLog.Append(bus.BusEvent{
				EventID:    bus.NewEventID(),
				Topic:      bus.TopicTick,
				Source:     "trader.md",
				OccurredAt: time.Now(),
				Payload:    payload,
			})
		},
		onBar: func(bar minuteBar) {
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
				Source:     "trader.bar",
				OccurredAt: bar.MinuteTime,
				Payload:    payload,
			})
		},
	}

	var spi *mdSpi
	if status != nil {
		spi = newMdSpiWithStatusAndOptions(store, l9Calc, status, options)
	} else {
		spi = newMdSpiWithOptions(store, l9Calc, options)
	}
	api := ctp.CThostFtdcMdApiCreateFtdcMdApi(s.cfg.FlowPath)

	api.RegisterSpi(ctp.NewDirectorCThostFtdcMdSpi(spi))
	api.RegisterFront(s.cfg.MdFrontAddr)
	api.Init()
	logger.Info("md api init done")

	time.Sleep(time.Duration(s.cfg.MdConnectWaitSeconds) * time.Second)

	if err := s.loginMd(api, 10001); err != nil {
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
		"api", "trader",
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
		"api", "trader",
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
		"api", "trader",
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
