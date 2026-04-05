package quotes

import (
	"math"
	"testing"
	"time"
)

func TestShouldCheckTickDriftNearTradingDayOnly(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 2, 15, 10, 0, 0, 0, time.Local)
	sameDay := time.Date(2026, 2, 15, 9, 59, 0, 0, time.Local)
	prevDay := time.Date(2026, 2, 14, 23, 59, 59, 0, time.Local)
	oldDay := time.Date(2026, 2, 10, 9, 0, 0, 0, time.Local)

	if !shouldCheckTickDrift(now, sameDay) {
		t.Fatal("same day tick should check drift")
	}
	if !shouldCheckTickDrift(now, prevDay) {
		t.Fatal("prev day tick should check drift")
	}
	if shouldCheckTickDrift(now, oldDay) {
		t.Fatal("old day tick should not check drift")
	}
}

func TestSanitizeSettlementPriceResetsExtremeValueToZero(t *testing.T) {
	t.Parallel()

	if got := sanitizeSettlementPrice(math.MaxFloat64); got != 0 {
		t.Fatalf("sanitizeSettlementPrice(MaxFloat64) = %v, want 0", got)
	}
	if got := sanitizeSettlementPrice(5427.25); got != 5427.25 {
		t.Fatalf("sanitizeSettlementPrice(normal) = %v, want 5427.25", got)
	}
}

func TestComputeBucketVolume(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		currentVol int
		prevVol    int
		hasPrev    bool
		want       int64
	}{
		{name: "first generated minute uses current volume", currentVol: 12, prevVol: 0, hasPrev: false, want: 12},
		{name: "normal delta", currentVol: 28, prevVol: 20, hasPrev: true, want: 8},
		{name: "counter reset falls back to current", currentVol: 6, prevVol: 20, hasPrev: true, want: 6},
		{name: "negative current clamps to zero", currentVol: -1, prevVol: 20, hasPrev: true, want: 0},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := computeBucketVolume(tc.currentVol, tc.prevVol, tc.hasPrev); got != tc.want {
				t.Fatalf("computeBucketVolume(%d,%d,%v)=%d want %d", tc.currentVol, tc.prevVol, tc.hasPrev, got, tc.want)
			}
		})
	}
}
