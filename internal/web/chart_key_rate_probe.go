package web

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
	data map[string]keyedRateSnapshot
}

type keyedRateSnapshot struct {
	count        int64
	matchedConns int
}

func newKeyedRateProbe(stage string) *keyedRateProbe {
	return &keyedRateProbe{
		stage: stage,
		data:  make(map[string]keyedRateSnapshot),
	}
}

func (p *keyedRateProbe) Add(key string, matchedConns int) {
	if p == nil {
		return
	}
	if key == "" {
		return
	}
	p.once.Do(func() {
		go p.loop()
	})
	p.mu.Lock()
	item := p.data[key]
	item.count++
	item.matchedConns = matchedConns
	p.data[key] = item
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
	p.data = make(map[string]keyedRateSnapshot)
	p.mu.Unlock()

	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		item := snapshot[key]
		logger.Debug("chart key rate", "stage", p.stage, "subscription_key", key, "count_per_sec", item.count, "matched_conns", item.matchedConns)
	}
}

var broadcastChartUpdateKeyRateProbe = newKeyedRateProbe("broadcastChartUpdate_by_key")
