package trader

import (
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
