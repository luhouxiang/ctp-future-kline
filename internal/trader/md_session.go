package trader

import (
	"math/rand"
	"sync"
	"time"

	"ctp-go-demo/internal/config"
	"ctp-go-demo/internal/logger"
)

type mdSessionOps struct {
	login         func() error
	subscribe     func() error
	now           func() time.Time
	sleep         func(time.Duration)
	logWarnNoTick func()
}

type mdSession struct {
	cfg              config.CTPConfig
	status           *RuntimeStatusCenter
	subscribeTargets []string
	ops              mdSessionOps

	disconnectCh chan int

	mu            sync.Mutex
	reconnecting  bool
	networkWarned bool
	rng           *rand.Rand
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
	return &mdSession{
		cfg:              cfg,
		status:           status,
		subscribeTargets: append([]string(nil), subscribeTargets...),
		ops:              ops,
		disconnectCh:     make(chan int, 16),
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *mdSession) Start() {
	go s.disconnectLoop()
	go s.noTickMonitorLoop()
}

func (s *mdSession) NotifyDisconnected(reason int) {
	select {
	case s.disconnectCh <- reason:
	default:
	}
}

func (s *mdSession) disconnectLoop() {
	for reason := range s.disconnectCh {
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
