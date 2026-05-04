package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"ctp-future-kline/internal/config"
	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/strategy"
)

func main() {
	configPath := flag.String("config", filepath.Join("config", "config.json"), "config file path")
	tablesRaw := flag.String("tables", "", "comma separated kline table names")
	algorithmsRaw := flag.String("algorithms", "", "comma separated algorithms: baseline,hard_filter,score_filter")
	startRaw := flag.String("start", "", "start time, RFC3339 or yyyy-mm-dd HH:MM:SS")
	endRaw := flag.String("end", "", "end time, RFC3339 or yyyy-mm-dd HH:MM:SS")
	outPath := flag.String("out", "", "optional JSON output path")
	limit := flag.Int("attempt-limit", 2000, "max detailed attempts in JSON output")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal(err)
	}
	db, err := dbx.Open(dbx.DSNForRole(cfg.DB, dbx.RoleMarketRealtime))
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	btCfg := strategy.DefaultMA20BacktestConfig()
	if strings.TrimSpace(*tablesRaw) != "" {
		btCfg.Tables = strings.Split(*tablesRaw, ",")
	}
	if strings.TrimSpace(*algorithmsRaw) != "" {
		btCfg.Algorithms = strings.Split(*algorithmsRaw, ",")
	}
	btCfg.ReportAttemptLimit = *limit
	if strings.TrimSpace(*startRaw) != "" {
		ts, err := parseTimeArg(*startRaw)
		if err != nil {
			fatal(err)
		}
		btCfg.StartTime = ts
	}
	if strings.TrimSpace(*endRaw) != "" {
		ts, err := parseTimeArg(*endRaw)
		if err != nil {
			fatal(err)
		}
		btCfg.EndTime = ts
	}

	resp, err := strategy.RunMA20Backtest(context.Background(), db, btCfg)
	if err != nil {
		fatal(err)
	}
	body, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		fatal(err)
	}
	if strings.TrimSpace(*outPath) != "" {
		if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil && filepath.Dir(*outPath) != "." {
			fatal(err)
		}
		if err := os.WriteFile(*outPath, body, 0o644); err != nil {
			fatal(err)
		}
	}
	printSummary(resp)
	if strings.TrimSpace(*outPath) == "" {
		fmt.Println(string(body))
	}
}

func parseTimeArg(raw string) (time.Time, error) {
	value := strings.TrimSpace(raw)
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
	}
	for _, layout := range layouts {
		if ts, err := time.ParseInLocation(layout, value, time.Local); err == nil {
			return ts, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time: %s", raw)
}

func printSummary(resp strategy.BacktestResponse) {
	fmt.Println("MA20 weak pullback short backtest")
	stats, _ := resp.Summary["stats"].(map[string]strategy.MA20BacktestStats)
	if len(stats) == 0 {
		raw, _ := json.Marshal(resp.Summary["stats"])
		fmt.Printf("stats: %s\n", raw)
		return
	}
	for _, algo := range strategy.DefaultMA20BacktestAlgorithms {
		s, ok := stats[algo]
		if !ok {
			continue
		}
		fmt.Printf(
			"%s attempts=%d filtered=%d signals=%d success=%d failure=%d unresolved=%d attempt_rate=%.4f signal_rate=%.4f formation=%.4f\n",
			algo,
			s.AttemptsStarted,
			s.Filtered,
			s.Signals,
			s.Success,
			s.Failure,
			s.Unresolved,
			s.AttemptSuccessRate,
			s.SignalSuccessRate,
			s.SignalFormationRate,
		)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
