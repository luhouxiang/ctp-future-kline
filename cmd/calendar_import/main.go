package main

import (
	"flag"
	"fmt"
	"os"

	"ctp-go-demo/internal/calendar"
	"ctp-go-demo/internal/config"
	dbx "ctp-go-demo/internal/db"
)

func main() {
	configPath := flag.String("config", "../ctp-future-resources/config/config.json", "config file path")
	source := flag.String("source", "csv", "import source: csv|url|shfe")
	csvPath := flag.String("csv", "", "calendar csv file path")
	url := flag.String("url", "", "calendar csv url")
	minFuture := flag.Int("min-future-days", 60, "future open days threshold for status")
	browserFallback := flag.Bool("browser-fallback", true, "enable browser fallback when SHFE blocks HTTP client")
	browserPath := flag.String("browser-path", "", "optional browser executable path for SHFE fetch")
	browserHeadless := flag.Bool("browser-headless", true, "run browser fallback in headless mode")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load config failed:", err)
		os.Exit(1)
	}
	if err := dbx.EnsureDatabase(cfg.DB); err != nil {
		fmt.Fprintln(os.Stderr, "ensure mysql database failed:", err)
		os.Exit(1)
	}
	dsn := dbx.BuildDSN(cfg.DB)
	db, err := dbx.Open(dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open mysql failed:", err)
		os.Exit(1)
	}
	if err := dbx.EnsureDatabaseAndSchema(cfg.DB, db); err != nil {
		_ = db.Close()
		fmt.Fprintln(os.Stderr, "ensure mysql schema failed:", err)
		os.Exit(1)
	}
	_ = db.Close()

	m := calendar.NewManager(dsn)
	switch *source {
	case "shfe":
		err = m.RefreshIfNeeded(calendar.Config{
			AutoUpdateOnStart:  true,
			MinFutureOpenDays:  1000000, // force manual refresh path by making horizon always true
			SourceURL:          *url,
			CheckIntervalHours: 0,
			BrowserFallback:    *browserFallback,
			BrowserPath:        *browserPath,
			BrowserHeadless:    *browserHeadless,
		})
	case "csv":
		if *csvPath == "" {
			fmt.Fprintln(os.Stderr, "-csv is required when -source=csv")
			os.Exit(2)
		}
		err = m.ImportCSVFile(*csvPath)
	case "url":
		if *url == "" {
			fmt.Fprintln(os.Stderr, "-url is required when -source=url")
			os.Exit(2)
		}
		err = m.ImportFromURL(*url)
	default:
		fmt.Fprintln(os.Stderr, "invalid -source, expected csv|url|shfe")
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "import failed:", err)
		os.Exit(1)
	}

	st, err := m.Status(*minFuture)
	if err != nil {
		fmt.Fprintln(os.Stderr, "status failed:", err)
		os.Exit(1)
	}
	fmt.Printf("ok total=%d open=%d min=%s max=%s needs_update=%v\n", st.TotalRows, st.OpenRows, st.MinDate, st.MaxDate, st.NeedsAutoUpdate)
}
