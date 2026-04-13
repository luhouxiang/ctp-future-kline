package quotes

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestArchiveTickDirOnStartupArchivesOldTradingDay(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	tickDir := filepath.Join(baseDir, "ticks")
	if err := os.MkdirAll(tickDir, 0o755); err != nil {
		t.Fatalf("mkdir tick dir failed: %v", err)
	}
	writeTickArchiveCSV(t, filepath.Join(tickDir, "rb2505.csv"), "20260411")

	now := time.Date(2026, 4, 12, 16, 30, 0, 0, time.Local)
	if err := ArchiveTickDirOnStartup(baseDir, now); err != nil {
		t.Fatalf("ArchiveTickDirOnStartup() error = %v", err)
	}

	if _, err := os.Stat(tickDir); !os.IsNotExist(err) {
		t.Fatalf("tick dir still exists after archive, err = %v", err)
	}

	archiveDir := filepath.Join(baseDir, "ticks-20260411")
	if info, err := os.Stat(archiveDir); err != nil {
		t.Fatalf("archive dir missing: %v", err)
	} else if !info.IsDir() {
		t.Fatalf("archive path is not dir: %s", archiveDir)
	}
	if _, err := os.Stat(filepath.Join(archiveDir, "rb2505.csv")); err != nil {
		t.Fatalf("archived csv missing: %v", err)
	}
}

func TestArchiveTickDirOnStartupSkipsCurrentTradingDay(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	tickDir := filepath.Join(baseDir, "ticks")
	if err := os.MkdirAll(tickDir, 0o755); err != nil {
		t.Fatalf("mkdir tick dir failed: %v", err)
	}
	writeTickArchiveCSV(t, filepath.Join(tickDir, "rb2505.csv"), "20260412")
	archiveDir := filepath.Join(baseDir, "ticks-20260412")
	if err := os.MkdirAll(archiveDir, 0o755); err != nil {
		t.Fatalf("write existing archive dir failed: %v", err)
	}

	now := time.Date(2026, 4, 12, 16, 30, 0, 0, time.Local)
	if err := ArchiveTickDirOnStartup(baseDir, now); err != nil {
		t.Fatalf("ArchiveTickDirOnStartup() error = %v", err)
	}

	if _, err := os.Stat(tickDir); err != nil {
		t.Fatalf("tick dir missing after skip: %v", err)
	}
	if _, err := os.Stat(archiveDir); err != nil {
		t.Fatalf("existing archive dir missing after skip: %v", err)
	}
}

func TestArchiveTickDirOnStartupSkipsBeforeCutoff(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	tickDir := filepath.Join(baseDir, "ticks")
	if err := os.MkdirAll(tickDir, 0o755); err != nil {
		t.Fatalf("mkdir tick dir failed: %v", err)
	}
	writeTickArchiveCSV(t, filepath.Join(tickDir, "rb2505.csv"), "20260411")

	now := time.Date(2026, 4, 12, 15, 59, 0, 0, time.Local)
	if err := ArchiveTickDirOnStartup(baseDir, now); err != nil {
		t.Fatalf("ArchiveTickDirOnStartup() error = %v", err)
	}

	if _, err := os.Stat(tickDir); err != nil {
		t.Fatalf("tick dir missing before cutoff: %v", err)
	}
}

func writeTickArchiveCSV(t *testing.T, path string, tradingDay string) {
	t.Helper()
	content := "received_at,instrument_id,exchange_id,exchange_inst_id,trading_day,action_day,update_time,last_price,pre_settlement_price,pre_close_price,pre_open_interest,open_price,highest_price,lowest_price,volume,turnover,open_interest,close_price,settlement_price,upper_limit_price,lower_limit_price,average_price,pre_delta,curr_delta,bid_price1,ask_price1,update_millisec,bid_volume1,ask_volume1\n" +
		"2026-04-12 09:30:00.000,rb2505,SHFE,rb2505," + tradingDay + ",20260412,09:30:00,3200,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,3199,3201,0,10,10\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write tick csv failed: %v", err)
	}
}
