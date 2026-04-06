package quotes

import (
	"testing"
	"time"
)

func TestNormalizeChartSubscription(t *testing.T) {
	sub, err := NormalizeChartSubscription(ChartSubscription{
		Symbol:    "AGL9",
		Type:      "",
		Variety:   "",
		Timeframe: "1M",
	})
	if err != nil {
		t.Fatalf("NormalizeChartSubscription error: %v", err)
	}
	if sub.Symbol != "agl9" || sub.Type != "l9" || sub.Variety != "ag" || sub.Timeframe != "1m" || sub.DataMode != "realtime" {
		t.Fatalf("unexpected normalized subscription: %+v", sub)
	}
}

func TestChartSubscriptionKeyStable(t *testing.T) {
	key1 := ChartSubscriptionKey(ChartSubscription{Symbol: "agl9", Type: "l9", Variety: "ag", Timeframe: "1m", DataMode: "replay"})
	key2 := ChartSubscriptionKey(ChartSubscription{Symbol: "AGL9", Type: "L9", Variety: "AG", Timeframe: "1M", DataMode: "REPLAY"})
	if key1 != key2 {
		t.Fatalf("ChartSubscriptionKey should normalize input, want %s got %s", key1, key2)
	}
	sub, err := NormalizeChartSubscription(ChartSubscription{Symbol: "AGL9", Type: "L9", Variety: "AG", Timeframe: "1M", DataMode: "REPLAY"})
	if err != nil {
		t.Fatalf("NormalizeChartSubscription error: %v", err)
	}
	if key1 != ChartSubscriptionKey(sub) {
		t.Fatalf("normalized key mismatch: want %s got %s", key1, ChartSubscriptionKey(sub))
	}
}

func TestSnapshotQuoteContractIncludesRecentTicks(t *testing.T) {
	stream := &ChartStream{
		roots: make(map[string]*chartRootState),
	}
	root := &chartRootState{
		symbol:  "ag2605",
		kind:    "contract",
		variety: "ag",
		history1m: []minuteBar{{
			InstrumentID: "ag2605",
			Variety:      "ag",
			Period:       "1m",
			MinuteTime:   time.Date(2026, 4, 5, 21, 1, 0, 0, time.Local),
			Open:         17100,
			High:         17120,
			Low:          17090,
			Close:        17110,
			Volume:       12,
		}},
		latestTicks: map[string]chartTickSnapshot{
			"ag2605": {
				price:              17111,
				preSettlementPrice: 17100,
				preClosePrice:      17098,
				openPrice:          17090,
				highestPrice:       17120,
				lowestPrice:        17080,
				volume:             120,
				turnover:           2500000,
				openInterest:       12345,
				settlementPrice:    17108,
				upperLimitPrice:    18000,
				lowerLimitPrice:    16000,
				averagePrice:       17105,
				bidPrice1:          17110,
				bidVolume1:         8,
				askPrice1:          17112,
				askVolume1:         9,
				adjustedTick:       time.Date(2026, 4, 5, 21, 1, 9, 0, time.Local),
				updateTime:         "21:01:09",
			},
		},
		recentTicks: []chartQuoteTickRow{
			{at: time.Date(2026, 4, 5, 21, 1, 9, 0, time.Local), price: 17111, volume: 120, openInterest: 12345, bidPrice1: 17110, askPrice1: 17112, dataMode: "replay"},
			{at: time.Date(2026, 4, 5, 21, 1, 8, 0, time.Local), price: 17110, volume: 116, openInterest: 12340, bidPrice1: 17109, askPrice1: 17111, dataMode: "replay"},
		},
	}
	stream.roots[rootKeyForSubscription(ChartSubscription{Symbol: "ag2605", Type: "contract", Variety: "ag"})] = root
	update, ok := stream.SnapshotQuote(ChartSubscription{Symbol: "ag2605", Type: "contract", Variety: "ag", Timeframe: "5m", DataMode: "replay"})
	if !ok {
		t.Fatal("SnapshotQuote returned no update")
	}
	if update.Subscription.Timeframe != "5m" {
		t.Fatalf("unexpected timeframe: %+v", update.Subscription)
	}
	if update.Snapshot.LatestPrice == nil || *update.Snapshot.LatestPrice != 17111 {
		t.Fatalf("unexpected latest price: %+v", update.Snapshot)
	}
	if update.Snapshot.BidVolume1 == nil || *update.Snapshot.BidVolume1 != 8 {
		t.Fatalf("unexpected bid volume1: %+v", update.Snapshot)
	}
	if update.Snapshot.CurrentVolume == nil || *update.Snapshot.CurrentVolume != 4 {
		t.Fatalf("unexpected current volume: %+v", update.Snapshot)
	}
	if update.Snapshot.OIDelta == nil || *update.Snapshot.OIDelta != 5 {
		t.Fatalf("unexpected oi delta: %+v", update.Snapshot)
	}
	if update.Snapshot.Change == nil || *update.Snapshot.Change != 11 {
		t.Fatalf("unexpected change: %+v", update.Snapshot)
	}
	if len(update.Ticks) != 2 || update.Ticks[0].Time != "21:01:09" {
		t.Fatalf("unexpected ticks: %+v", update.Ticks)
	}
	if update.Ticks[0].Volume == nil || *update.Ticks[0].Volume != 4 {
		t.Fatalf("unexpected tick volume: %+v", update.Ticks)
	}
	if update.Ticks[0].OIDelta == nil || *update.Ticks[0].OIDelta != 5 {
		t.Fatalf("unexpected tick oi delta: %+v", update.Ticks)
	}
	if update.Ticks[0].Nature != "中性" {
		t.Fatalf("unexpected tick nature: %+v", update.Ticks[0])
	}
}

func TestSnapshotBarUsesLatestCachedPhase(t *testing.T) {
	t.Parallel()

	stream := &ChartStream{
		roots: make(map[string]*chartRootState),
	}
	root := &chartRootState{
		symbol:       "ag2605",
		kind:         "contract",
		variety:      "ag",
		latestBars:   make(map[string]minuteBar),
		latestPhases: make(map[string]string),
	}
	root.latestBars["5m"] = minuteBar{
		Variety:      "ag",
		InstrumentID: "ag2605",
		Exchange:     "SHFE",
		MinuteTime:   time.Date(2026, 4, 5, 21, 5, 0, 0, time.Local),
		AdjustedTime: time.Date(2026, 4, 5, 21, 5, 0, 0, time.Local),
		Period:       "5m",
		Open:         100,
		High:         105,
		Low:          99,
		Close:        104,
		Volume:       20,
	}
	root.latestPhases["5m"] = "partial"
	stream.roots[rootKeyForSubscription(ChartSubscription{Symbol: "ag2605", Type: "contract", Variety: "ag"})] = root

	update, ok := stream.SnapshotBar(ChartSubscription{Symbol: "ag2605", Type: "contract", Variety: "ag", Timeframe: "5m", DataMode: "realtime"})
	if !ok {
		t.Fatal("SnapshotBar returned no update")
	}
	if update.Phase != "partial" {
		t.Fatalf("phase=%s want partial", update.Phase)
	}
	if update.Bar.AdjustedTime != time.Date(2026, 4, 5, 21, 5, 0, 0, time.Local).Unix() {
		t.Fatalf("adjusted_time=%d want %d", update.Bar.AdjustedTime, time.Date(2026, 4, 5, 21, 5, 0, 0, time.Local).Unix())
	}
}
