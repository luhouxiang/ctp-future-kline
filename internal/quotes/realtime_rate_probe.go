package quotes

import (
	"sync"
	"sync/atomic"
	"time"

	"ctp-future-kline/internal/logger"
)

type rateProbe struct {
	stage string
	count atomic.Int64
	once  sync.Once
}

func newRateProbe(stage string) *rateProbe {
	return &rateProbe{stage: stage}
}

func (p *rateProbe) Inc() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		go p.loop()
	})
	p.count.Add(1)
}

func (p *rateProbe) loop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		n := p.count.Swap(0)
		logger.Info("realtime rate", "stage", p.stage, "count_per_sec", n)
	}
}

var (
	onRtnDepthMarketDataRateProbe = newRateProbe("OnRtnDepthMarketData")
	onPartialBarRateProbe         = newRateProbe("onPartialBar")
)
