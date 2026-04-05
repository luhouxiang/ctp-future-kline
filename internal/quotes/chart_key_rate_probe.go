package quotes

import (
	"sort"
	"sync"
	"time"

	"ctp-future-kline/internal/logger"
)

type keyedRateProbe struct {
	stage string

	once sync.Once
	mu   sync.Mutex
	data map[string]int64
}

func newKeyedRateProbe(stage string) *keyedRateProbe {
	return &keyedRateProbe{
		stage: stage,
		data:  make(map[string]int64),
	}
}

func (p *keyedRateProbe) Inc(key string) {
	if p == nil {
		return
	}
	key = normalizeProbeKey(key)
	if key == "" {
		return
	}
	p.once.Do(func() {
		go p.loop()
	})
	p.mu.Lock()
	p.data[key]++
	p.mu.Unlock()
}

func (p *keyedRateProbe) loop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		p.flush()
	}
}

func (p *keyedRateProbe) flush() {
	p.mu.Lock()
	if len(p.data) == 0 {
		p.mu.Unlock()
		return
	}
	snapshot := p.data
	p.data = make(map[string]int64)
	p.mu.Unlock()

	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		logger.Debug("chart key rate", "stage", p.stage, "subscription_key", key, "count_per_sec", snapshot[key])
	}
}

func normalizeProbeKey(key string) string {
	return key
}

var chartPartialKeyRateProbe = newKeyedRateProbe("chart_partial_generated")
