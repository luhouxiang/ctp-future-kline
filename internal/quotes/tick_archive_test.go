package quotes

import (
	"archive/zip"
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

	zipPath := filepath.Join(baseDir, "ticks-20260411.zip")
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		t.Fatalf("open archive zip failed: %v", err)
	}
	defer reader.Close()

	if len(reader.File) != 1 {
		t.Fatalf("zip file count = %d, want 1", len(reader.File))
	}
	if reader.File[0].Name != "ticks-20260411/rb2505.csv" {
		t.Fatalf("zip entry = %q, want ticks-20260411/rb2505.csv", reader.File[0].Name)
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
	zipPath := filepath.Join(baseDir, "ticks-20260412.zip")
	if err := os.WriteFile(zipPath, []byte("stub"), 0o644); err != nil {
		t.Fatalf("write existing zip failed: %v", err)
	}

	now := time.Date(2026, 4, 12, 16, 30, 0, 0, time.Local)
	if err := ArchiveTickDirOnStartup(baseDir, now); err != nil {
		t.Fatalf("ArchiveTickDirOnStartup() error = %v", err)
	}

	if _, err := os.Stat(tickDir); err != nil {
		t.Fatalf("tick dir missing after skip: %v", err)
	}
	if _, err := os.Stat(zipPath); err != nil {
		t.Fatalf("existing archive zip missing after skip: %v", err)
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
