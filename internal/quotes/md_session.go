// md_session.go 管理行情订阅会话的保活逻辑。
// 它负责断线后的指数退避重连、重新登录与重订阅，以及长时间无 tick 的网络异常告警。
package quotes

import (
	"math/rand"
	"sync"
	"time"

	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/queuewatch"
)

type mdSessionOps struct {
	// login 在重连流程中触发一次 MD 登录。
	login func() error
	// subscribe 在登录成功后重新发起订阅。
	subscribe func() error
	// now 允许测试注入当前时间。
	now func() time.Time
	// sleep 允许测试替换等待逻辑。
	sleep func(time.Duration)
	// logWarnNoTick 在长时间无 tick 时输出告警。
	logWarnNoTick func()
}

type mdSession struct {
	// cfg 保存行情会话的重连和告警配置。
	cfg config.CTPConfig
	// status 用于向外同步断线、重连和网络怀疑状态。
	status *RuntimeStatusCenter
	// subscribeTargets 保存断线重连后需要重新订阅的合约列表。
	subscribeTargets []string
	// ops 保存可替换的重连动作和时间函数。
	ops mdSessionOps

	// disconnectCh 接收 mdSpi 上报的断线 reason。
	disconnectCh chan int
	// disconnectQueue 汇总断线信号队列的深度和丢弃情况。
	disconnectQueue *queuewatch.QueueHandle

	// mu 保护 reconnecting、networkWarned 和 rng。
	mu sync.Mutex
	// reconnecting 表示当前是否已经有重连流程在跑。
	reconnecting bool
	// networkWarned 用于避免连续重复打印无 tick 告警。
	networkWarned bool
	// rng 用于给退避等待注入随机抖动。
	rng *rand.Rand
}

func newMDSession(cfg config.CTPConfig, status *RuntimeStatusCenter, subscribeTargets []string, ops mdSessionOps) *mdSession {
	if ops.now == nil {
		ops.now = time.Now
	}
	if ops.sleep == nil {
		ops.sleep = time.Sleep
	}
	if ops.logWarnNoTick == nil {
		ops.logWarnNoTick = func() {
			logger.Error("md no tick warning", "no_tick_warn_seconds", cfg.NoTickWarnSeconds)
		}
	}
	queueCfg := queuewatch.DefaultConfig("")
	registry := (*queuewatch.Registry)(nil)
	if status != nil && status.QueueRegistry() != nil {
		registry = status.QueueRegistry()
		queueCfg = registry.Config()
	}
	session := &mdSession{
		cfg:              cfg,
		status:           status,
		subscribeTargets: append([]string(nil), subscribeTargets...),
		ops:              ops,
		disconnectCh:     make(chan int, queueCfg.MDDisconnectCapacity),
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if registry != nil {
		session.disconnectQueue = registry.Register(queuewatch.QueueSpec{
			Name:        "md_disconnect_signals",
			Category:    "quotes_sidecar",
			Criticality: "best_effort",
			Capacity:    queueCfg.MDDisconnectCapacity,
			LossPolicy:  "best_effort",
			BasisText:   "行情断线信号单独排队，容量等于 md_disconnect_capacity，用于在重连触发前吸收短时抖动。",
		})
	}
	return session
}

func (s *mdSession) Start() {
	go s.disconnectLoop()
	go s.noTickMonitorLoop()
}

func (s *mdSession) NotifyDisconnected(reason int) {
	select {
	case s.disconnectCh <- reason:
		if s.disconnectQueue != nil {
			s.disconnectQueue.MarkEnqueued(len(s.disconnectCh))
		}
	default:
		if s.disconnectQueue != nil {
			s.disconnectQueue.MarkDropped(len(s.disconnectCh))
		}
	}
}

func (s *mdSession) disconnectLoop() {
	for reason := range s.disconnectCh {
		if s.disconnectQueue != nil {
			s.disconnectQueue.MarkDequeued(len(s.disconnectCh))
		}
		if s.status != nil {
			s.status.MarkMdFrontDisconnected(reason)
		}
		if !s.cfg.IsMdReconnectEnabled() {
			continue
		}
		if !s.tryStartReconnect() {
			continue
		}
		go s.reconnectLoop()
	}
}

func (s *mdSession) noTickMonitorLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if s.status == nil {
			continue
		}
		now := s.ops.now()
		snapshot := s.status.Snapshot(now)
		noTickWarn := time.Duration(s.cfg.NoTickWarnSeconds) * time.Second
		suspect := snapshot.MdFront && snapshot.MdLogin && !snapshot.LastTickTime.IsZero() && now.Sub(snapshot.LastTickTime) > noTickWarn
		if suspect {
			if !s.getAndSetNetworkWarned(true) {
				s.ops.logWarnNoTick()
			}
		} else {
			s.getAndSetNetworkWarned(false)
		}
		s.status.MarkNetworkSuspect(suspect)
	}
}

func (s *mdSession) reconnectLoop() {
	defer s.stopReconnect()

	for attempt := 1; ; attempt++ {
		delay := s.nextBackoff(attempt)
		nextRetryAt := s.ops.now().Add(delay)
		if s.status != nil {
			s.status.MarkMdReconnectAttempt(attempt, nextRetryAt)
		}
		s.ops.sleep(delay)

		if err := s.ops.login(); err != nil {
			logger.Error("md reconnect login failed", "attempt", attempt, "error", err)
			continue
		}
		s.ops.sleep(time.Duration(s.cfg.MdReloginWaitSeconds) * time.Second)

		if err := s.ops.subscribe(); err != nil {
			logger.Error("md reconnect subscribe failed", "attempt", attempt, "error", err)
			continue
		}

		logger.Info("md reconnect success", "attempt", attempt, "subscribe_count", len(s.subscribeTargets))
		if s.status != nil {
			s.status.MarkMdReconnectAttempt(0, time.Time{})
			s.status.MarkSubscribed(len(s.subscribeTargets))
		}
		return
	}
}

func (s *mdSession) tryStartReconnect() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reconnecting {
		return false
	}
	s.reconnecting = true
	return true
}

func (s *mdSession) stopReconnect() {
	s.mu.Lock()
	s.reconnecting = false
	s.mu.Unlock()
}

func (s *mdSession) getAndSetNetworkWarned(v bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.networkWarned
	s.networkWarned = v
	return prev
}

func (s *mdSession) nextBackoff(attempt int) time.Duration {
	base := s.cfg.MdReconnectInitialMS
	maxVal := s.cfg.MdReconnectMaxMS
	if base <= 0 {
		base = 1000
	}
	if maxVal < base {
		maxVal = base
	}
	if attempt < 1 {
		attempt = 1
	}
	delayMS := base
	for i := 1; i < attempt && delayMS < maxVal; i++ {
		delayMS *= 2
		if delayMS > maxVal {
			delayMS = maxVal
		}
	}
	delay := time.Duration(delayMS) * time.Millisecond
	jitterRatio := s.cfg.MdReconnectJitterRatio
	if jitterRatio <= 0 {
		return delay
	}

	s.mu.Lock()
	j := (s.rng.Float64()*2 - 1) * jitterRatio
	s.mu.Unlock()
	adjusted := float64(delay) * (1 + j)
	if adjusted < float64(time.Millisecond) {
		adjusted = float64(time.Millisecond)
	}
	return time.Duration(adjusted)
}
