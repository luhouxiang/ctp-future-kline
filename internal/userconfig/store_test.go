package userconfig

import (
	"testing"

	"ctp-future-kline/internal/config"
)

func TestApplyAppOverridesTradeEnabled(t *testing.T) {
	base := config.AppConfig{}
	base.Trade.Enabled = nil

	enabled := true
	out := ApplyAppOverrides(base, AppOverrides{
		Trade: TradeOverrides{Enabled: &enabled},
	})
	if out.Trade.Enabled == nil || !*out.Trade.Enabled {
		t.Fatalf("trade enabled override not applied: %#v", out.Trade.Enabled)
	}
}
