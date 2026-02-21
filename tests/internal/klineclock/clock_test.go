package klineclock_test

import (
	"testing"
	"time"

	"ctp-go-demo/internal/klineclock"
)

type fixedPrevProvider struct {
	prev time.Time
}

func (p fixedPrevProvider) PrevTradingDay(time.Time) (time.Time, error) {
	return p.prev, nil
}

func TestBuildBarTimes(t *testing.T) {
	t.Parallel()

	tradingDay, err := time.ParseInLocation("2006-01-02", "2026-02-09", time.Local)
	if err != nil {
		t.Fatal(err)
	}
	prev, err := time.ParseInLocation("2006-01-02", "2026-02-06", time.Local)
	if err != nil {
		t.Fatal(err)
	}
	provider := fixedPrevProvider{prev: prev}

	base, adjusted, err := klineclock.BuildBarTimes(tradingDay, 901, provider)
	if err != nil {
		t.Fatalf("BuildBarTimes(901) error: %v", err)
	}
	if base.Format("2006-01-02 15:04:05") != "2026-02-09 09:01:00" {
		t.Fatalf("base(901)=%s", base.Format("2006-01-02 15:04:05"))
	}
	if adjusted.Format("2006-01-02 15:04:05") != "2026-02-09 09:01:00" {
		t.Fatalf("adjusted(901)=%s", adjusted.Format("2006-01-02 15:04:05"))
	}

	base, adjusted, err = klineclock.BuildBarTimes(tradingDay, 2101, provider)
	if err != nil {
		t.Fatalf("BuildBarTimes(2101) error: %v", err)
	}
	if base.Format("2006-01-02 15:04:05") != "2026-02-09 21:01:00" {
		t.Fatalf("base(2101)=%s", base.Format("2006-01-02 15:04:05"))
	}
	if adjusted.Format("2006-01-02 15:04:05") != "2026-02-06 21:01:00" {
		t.Fatalf("adjusted(2101)=%s", adjusted.Format("2006-01-02 15:04:05"))
	}

	base, adjusted, err = klineclock.BuildBarTimes(tradingDay, 1, provider)
	if err != nil {
		t.Fatalf("BuildBarTimes(0001) error: %v", err)
	}
	if base.Format("2006-01-02 15:04:05") != "2026-02-09 00:01:00" {
		t.Fatalf("base(0001)=%s", base.Format("2006-01-02 15:04:05"))
	}
	if adjusted.Format("2006-01-02 15:04:05") != "2026-02-07 00:01:00" {
		t.Fatalf("adjusted(0001)=%s", adjusted.Format("2006-01-02 15:04:05"))
	}
}
