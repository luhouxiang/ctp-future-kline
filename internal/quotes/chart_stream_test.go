package quotes

import "testing"

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
	if sub.Symbol != "agl9" || sub.Type != "l9" || sub.Variety != "ag" || sub.Timeframe != "1m" {
		t.Fatalf("unexpected normalized subscription: %+v", sub)
	}
}

func TestChartSubscriptionKeyStable(t *testing.T) {
	key1 := ChartSubscriptionKey(ChartSubscription{Symbol: "agl9", Type: "l9", Variety: "ag", Timeframe: "1m"})
	key2 := ChartSubscriptionKey(ChartSubscription{Symbol: "AGL9", Type: "L9", Variety: "AG", Timeframe: "1M"})
	if key1 != key2 {
		t.Fatalf("ChartSubscriptionKey should normalize input, want %s got %s", key1, key2)
	}
	sub, err := NormalizeChartSubscription(ChartSubscription{Symbol: "AGL9", Type: "L9", Variety: "AG", Timeframe: "1M"})
	if err != nil {
		t.Fatalf("NormalizeChartSubscription error: %v", err)
	}
	if key1 != ChartSubscriptionKey(sub) {
		t.Fatalf("normalized key mismatch: want %s got %s", key1, ChartSubscriptionKey(sub))
	}
}
