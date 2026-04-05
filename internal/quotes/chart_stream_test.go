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
				price:           17111,
				bidPrice1:       17110,
				askPrice1:       17112,
				openInterest:    12345,
				settlementPrice: 17108,
				adjustedTick:    time.Date(2026, 4, 5, 21, 1, 9, 0, time.Local),
				updateTime:      "21:01:09",
			},
		},
		recentTicks: []chartQuoteTickRow{
			{at: time.Date(2026, 4, 5, 21, 1, 9, 0, time.Local), price: 17111, dataMode: "replay"},
			{at: time.Date(2026, 4, 5, 21, 1, 8, 0, time.Local), price: 17110, dataMode: "replay"},
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
	if len(update.Ticks) != 2 || update.Ticks[0].Time != "21:01:09" {
		t.Fatalf("unexpected ticks: %+v", update.Ticks)
	}
}
