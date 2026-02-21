package chartlayout

import (
	"encoding/json"
	"testing"
)

func TestChannelSettingsUnmarshalLegacy(t *testing.T) {
	var s ChannelSettings
	data := []byte(`{"window_size_minute":180,"pivot_k_minute":9,"residual_atr_factor":1.2,"slope_tol_atr_factor":0.22,"hide_auto":true}`)
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("unmarshal legacy failed: %v", err)
	}
	if s.Common.WindowSizeMinute != 180 {
		t.Fatalf("window minute = %d, want 180", s.Common.WindowSizeMinute)
	}
	if s.Algorithms.Ransac.PivotKMinute != 9 || s.Algorithms.Extrema.PivotKMinute != 9 {
		t.Fatalf("pivot mapping failed: ransac=%d extrema=%d", s.Algorithms.Ransac.PivotKMinute, s.Algorithms.Extrema.PivotKMinute)
	}
	if s.Algorithms.Ransac.ResidualAtrFactor != 1.2 {
		t.Fatalf("ransac residual = %v, want 1.2", s.Algorithms.Ransac.ResidualAtrFactor)
	}
	if s.Algorithms.Regression.ResidualAtrFactor != 1.2 {
		t.Fatalf("regression residual = %v, want 1.2", s.Algorithms.Regression.ResidualAtrFactor)
	}
	if !s.Common.HideAuto {
		t.Fatalf("hide_auto should be true")
	}
}

func TestChannelSettingsUnmarshalGroupedKeepsDefaults(t *testing.T) {
	var s ChannelSettings
	data := []byte(`{"display":{"show_ransac":true}}`)
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("unmarshal grouped failed: %v", err)
	}
	if !s.Display.ShowRansac {
		t.Fatalf("show_ransac should be true")
	}
	if !s.Display.ShowExtrema {
		t.Fatalf("show_extrema should keep default true")
	}
	if !s.Common.LiveApply {
		t.Fatalf("live_apply should keep default true")
	}
}
