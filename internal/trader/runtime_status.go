package trader

import (
	"sync"
	"time"
)

const (
	RuntimeStateIdle     = "idle"
	RuntimeStateStarting = "starting"
	RuntimeStateRunning  = "running"
	RuntimeStateError    = "error"
)

type RuntimeSnapshot struct {
	State          string    `json:"state"`
	TraderFront    bool      `json:"trader_front"`
	TraderLogin    bool      `json:"trader_login"`
	MdFront        bool      `json:"md_front"`
	MdLogin        bool      `json:"md_login"`
	MdSubscribed   bool      `json:"md_subscribed"`
	MdFrontDown    bool      `json:"md_front_disconnected"`
	MdDownReason   int       `json:"md_disconnect_reason"`
	MdReconnectTry int       `json:"md_reconnect_attempt"`
	MdNextRetryAt  time.Time `json:"md_next_retry_at"`
	NetworkSuspect bool      `json:"network_suspect"`
	TickDedupDrop  int64     `json:"tick_dedup_dropped"`
	DriftSeconds   float64   `json:"drift_seconds"`
	DriftPaused    bool      `json:"drift_paused"`
	DriftPauseCnt  int64     `json:"drift_pause_count"`
	ServerTime     string    `json:"server_time"`
	LastTickTime   time.Time `json:"last_tick_time"`
	IsMarketOpen   bool      `json:"is_market_open"`
	LastError      string    `json:"last_error"`
	UpdatedAt      time.Time `json:"updated_at"`
	TradingDay     string    `json:"trading_day"`
	SubscribeCount int       `json:"subscribe_count"`
}

type RuntimeStatusCenter struct {
	mu                sync.RWMutex
	marketOpenStale   time.Duration
	snapshot          RuntimeSnapshot
	subscribers       map[chan RuntimeSnapshot]struct{}
	subscriberBufSize int
}

func NewRuntimeStatusCenter(marketOpenStale time.Duration) *RuntimeStatusCenter {
	return &RuntimeStatusCenter{
		marketOpenStale:   marketOpenStale,
		snapshot:          RuntimeSnapshot{State: RuntimeStateIdle, UpdatedAt: time.Now()},
		subscribers:       make(map[chan RuntimeSnapshot]struct{}),
		subscriberBufSize: 16,
	}
}

func (c *RuntimeStatusCenter) Snapshot(now time.Time) RuntimeSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := c.snapshot
	if out.LastTickTime.IsZero() {
		out.IsMarketOpen = false
		return out
	}
	out.IsMarketOpen = now.Sub(out.LastTickTime) <= c.marketOpenStale
	return out
}

func (c *RuntimeStatusCenter) Subscribe() (<-chan RuntimeSnapshot, func()) {
	ch := make(chan RuntimeSnapshot, c.subscriberBufSize)

	c.mu.Lock()
	c.subscribers[ch] = struct{}{}
	current := c.snapshot
	c.mu.Unlock()

	ch <- current

	cancel := func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, ok := c.subscribers[ch]; ok {
			delete(c.subscribers, ch)
			close(ch)
		}
	}
	return ch, cancel
}

func (c *RuntimeStatusCenter) SetStarting() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.State = RuntimeStateStarting
		s.LastError = ""
	})
}

func (c *RuntimeStatusCenter) SetRunning() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.State = RuntimeStateRunning
		s.LastError = ""
	})
}

func (c *RuntimeStatusCenter) SetError(err error) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.State = RuntimeStateError
		if err != nil {
			s.LastError = err.Error()
		}
	})
}

func (c *RuntimeStatusCenter) MarkTraderFrontConnected() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.TraderFront = true
	})
}

func (c *RuntimeStatusCenter) MarkTraderLogin(loginTime string, tradingDay string) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.TraderLogin = true
		if loginTime != "" {
			s.ServerTime = loginTime
		}
		if tradingDay != "" {
			s.TradingDay = tradingDay
		}
	})
}

func (c *RuntimeStatusCenter) MarkMdFrontConnected() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MdFront = true
		s.MdFrontDown = false
		s.MdDownReason = 0
	})
}

func (c *RuntimeStatusCenter) MarkMdLogin(loginTime string, tradingDay string) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MdLogin = true
		s.MdFrontDown = false
		s.MdDownReason = 0
		s.MdReconnectTry = 0
		s.MdNextRetryAt = time.Time{}
		if loginTime != "" {
			s.ServerTime = loginTime
		}
		if tradingDay != "" {
			s.TradingDay = tradingDay
		}
	})
}

func (c *RuntimeStatusCenter) MarkSubscribed(count int) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MdSubscribed = true
		s.SubscribeCount = count
		s.NetworkSuspect = false
	})
}

func (c *RuntimeStatusCenter) MarkTick(ts time.Time) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.LastTickTime = ts
		s.ServerTime = ts.Format("2006-01-02 15:04:05")
		s.NetworkSuspect = false
	})
}

func (c *RuntimeStatusCenter) MarkMdFrontDisconnected(reason int) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MdFront = false
		s.MdLogin = false
		s.MdSubscribed = false
		s.MdFrontDown = true
		s.MdDownReason = reason
	})
}

func (c *RuntimeStatusCenter) MarkMdReconnectAttempt(attempt int, nextRetryAt time.Time) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.MdReconnectTry = attempt
		s.MdNextRetryAt = nextRetryAt
	})
}

func (c *RuntimeStatusCenter) MarkNetworkSuspect(v bool) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.NetworkSuspect = v
	})
}

func (c *RuntimeStatusCenter) MarkTickDedupDropped() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.TickDedupDrop++
	})
}

func (c *RuntimeStatusCenter) MarkDrift(driftSec float64, paused bool) {
	c.mutate(func(s *RuntimeSnapshot) {
		s.DriftSeconds = driftSec
		wasPaused := s.DriftPaused
		s.DriftPaused = paused
		if paused && !wasPaused {
			s.DriftPauseCnt++
		}
	})
}

func (c *RuntimeStatusCenter) MarkDriftRecovered() {
	c.mutate(func(s *RuntimeSnapshot) {
		s.DriftPaused = false
		s.DriftSeconds = 0
	})
}

func (c *RuntimeStatusCenter) mutate(fn func(*RuntimeSnapshot)) {
	c.mu.Lock()
	fn(&c.snapshot)
	c.snapshot.UpdatedAt = time.Now()
	out := c.snapshot
	if !out.LastTickTime.IsZero() {
		out.IsMarketOpen = time.Since(out.LastTickTime) <= c.marketOpenStale
	} else {
		out.IsMarketOpen = false
	}
	subs := make([]chan RuntimeSnapshot, 0, len(c.subscribers))
	for ch := range c.subscribers {
		subs = append(subs, ch)
	}
	c.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- out:
		default:
		}
	}
}
