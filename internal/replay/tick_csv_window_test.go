package replay

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"ctp-future-kline/internal/bus"
)

func TestInspectTickCSVWindowRespectsFilters(t *testing.T) {
	dir := t.TempDir()
	content := "ReceivedAt,InstrumentID,ExchangeID,TradingDay,ActionDay,UpdateTime,UpdateMillisec,LastPrice,Volume,OpenInterest,SettlementPrice,BidPrice1,AskPrice1\n" +
		"2026-03-27 21:00:00.000,ag2606,SHFE,20260330,20260327,21:00:00,0,100,1,10,0,99,101\n" +
		"2026-03-27 21:05:00.000,ag2606,SHFE,20260330,20260327,21:05:00,0,101,2,11,0,100,102\n" +
		"2026-03-27 21:10:00.000,ag2606,SHFE,20260330,20260327,21:10:00,0,102,3,12,0,101,103\n"
	if err := os.WriteFile(filepath.Join(dir, "ag2606.csv"), []byte(content), 0o644); err != nil {
		t.Fatalf("write tick csv failed: %v", err)
	}
	start := time.Date(2026, 3, 27, 21, 4, 0, 0, time.Local)
	end := time.Date(2026, 3, 27, 21, 10, 0, 0, time.Local)
	window, err := InspectTickCSVWindow(StartRequest{
		TickDir:    dir,
		StartTime:  &start,
		EndTime:    &end,
		FromCursor: &bus.FileCursor{File: "ag2606.csv", Offset: 3},
	})
	if err != nil {
		t.Fatalf("InspectTickCSVWindow error: %v", err)
	}
	if window.EventCount != 2 {
		t.Fatalf("unexpected event_count: got %d want 2", window.EventCount)
	}
	if window.StartTime == nil || window.EndTime == nil {
		t.Fatalf("window bounds should not be nil: %+v", window)
	}
	wantStart := time.Date(2026, 3, 27, 21, 5, 0, 0, time.Local)
	wantEnd := time.Date(2026, 3, 27, 21, 10, 0, 0, time.Local)
	if !window.StartTime.Equal(wantStart) {
		t.Fatalf("unexpected start_time: got %v want %v", window.StartTime, wantStart)
	}
	if !window.EndTime.Equal(wantEnd) {
		t.Fatalf("unexpected end_time: got %v want %v", window.EndTime, wantEnd)
	}
}
