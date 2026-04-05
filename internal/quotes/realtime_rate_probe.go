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
	// growthWindow 表示要连续观察多少个 1 秒窗口都在增长，
	// 才认为该 stage 的实时吞吐形成了明显上升趋势。
	const growthWindow = 30
	var (
		lastCount    int64
		hasLast      bool
		growthStreak int
		windowStart  int64
	)
	for range ticker.C {
		// 每秒原子取走并清空计数器，得到最近 1 秒的吞吐量，
		// 这样后续比较的是相邻秒窗口，而不是进程启动后的累计值。
		n := p.count.Swap(0)
		if hasLast && n > lastCount {
			if growthStreak == 0 {
				// 记录本轮连续增长的起点，便于告警时直接看出增长区间。
				windowStart = lastCount
			}
			growthStreak++
			if growthStreak >= growthWindow {
				logger.Info(
					"realtime rate increasing trend detected",
					"stage", p.stage,
					"seconds", growthWindow,
					"start_count_per_sec", windowStart,
					"end_count_per_sec", n,
				)
				// 命中一次连续增长趋势后立即清空状态，重新开始下一轮观察，
				// 避免同一段长趋势持续重复命中并刷屏。
				growthStreak = 0
				hasLast = false
				lastCount = 0
				windowStart = 0
				continue
			}
		} else {
			// 只要当前秒没有继续增长，就中断本轮趋势，等待新的连续增长窗口。
			growthStreak = 0
			windowStart = 0
		}
		lastCount = n
		hasLast = true
	}
}

var (
	onRtnDepthMarketDataRateProbe = newRateProbe("OnRtnDepthMarketData")
	onPartialBarRateProbe         = newRateProbe("onPartialBar")
)
