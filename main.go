package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"

	"ctp-go-demo/internal/calendar"
	"ctp-go-demo/internal/config"
	dbx "ctp-go-demo/internal/db"
	"ctp-go-demo/internal/logger"
	"ctp-go-demo/internal/version"
	"ctp-go-demo/internal/web"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("main panic recovered", "panic", r, "stack", string(debug.Stack()))
			os.Exit(1)
		}
	}()
	configPath := flag.String("config", "../ctp-future-resources/config/config.json", "config file path")
	noOpen := flag.Bool("no-open", false, "do not auto open browser")
	flag.Parse()
	logger.Info("backend version", "version", version.BackendVersion)

	resolvedConfigPath := resolveConfigPath(*configPath)
	if resolvedConfigPath != *configPath {
		logger.Info("config path fallback", "requested", *configPath, "resolved", resolvedConfigPath)
	}

	cfg, err := config.Load(resolvedConfigPath)
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}
	logPath := filepath.Join("..", "ctp-future-resources", "logs", "server.log")
	_ = os.MkdirAll(filepath.Dir(logPath), 0o755)
	if err := logger.InitFile(logPath); err != nil {
		logger.Error("init log file failed", "log_path", logPath, "error", err)
	}
	defer func() {
		_ = logger.Close()
	}()
	// Ensure the first line in file logs is the backend version.
	logger.Info("config loaded", "config_path", resolvedConfigPath)
	logger.Info("log file enabled", "log_path", logPath)
	if err := dbx.EnsureDatabase(cfg.DB); err != nil {
		logger.Error("ensure mysql database failed", "error", err)
	}
	dsn := dbx.BuildDSN(cfg.DB)
	db, err := dbx.Open(dsn)
	if err != nil {
		logger.Error("open mysql failed", "error", err)
		os.Exit(1)
	}
	if err := dbx.EnsureDatabaseAndSchema(cfg.DB, db); err != nil {
		logger.Error("ensure mysql schema failed", "error", err)
	}
	_ = db.Close()
	cfg.CTP.DBDSN = dsn
	cm := calendar.NewManager(dsn)
	if err := cm.EnsureOnStart(calendar.Config{
		AutoUpdateOnStart:  cfg.Calendar.IsAutoUpdateOnStart(),
		MinFutureOpenDays:  cfg.Calendar.MinFutureOpenDays,
		SourceURL:          cfg.Calendar.SourceURL,
		SourceCSVPath:      cfg.Calendar.SourceCSVPath,
		CheckIntervalHours: cfg.Calendar.CheckIntervalHours,
		BrowserFallback:    cfg.Calendar.IsBrowserFallbackEnabled(),
		BrowserPath:        cfg.Calendar.BrowserPath,
		BrowserHeadless:    cfg.Calendar.IsBrowserHeadless(),
	}); err != nil {
		logger.Error("calendar auto update failed", "error", err)
	}

	server := web.NewServer(cfg)
	url := fmt.Sprintf("http://%s", cfg.Web.ListenAddr)
	if cfg.Web.IsAutoOpenBrowser() && !*noOpen {
		if err := openBrowser(url); err != nil {
			logger.Error("open browser failed", "url", url, "error", err)
		}
	}

	tryFreeListenPort(cfg.Web.ListenAddr)
	if ln, err := net.Listen("tcp", cfg.Web.ListenAddr); err == nil {
		_ = ln.Close()
	} else {
		logger.Error("listen precheck failed", "addr", cfg.Web.ListenAddr, "error", err)
	}
	if err := server.Run(); err != nil {
		logger.Error("run web server failed", "error", err)
		os.Exit(1)
	}
}

func tryFreeListenPort(addr string) {
	if runtime.GOOS != "windows" {
		return
	}
	_, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 {
		return
	}
	// Use PowerShell to query listener PIDs on target port.
	cmd := exec.Command("powershell", "-NoProfile", "-Command",
		fmt.Sprintf("(Get-NetTCPConnection -LocalPort %d -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique) -join \"`n\"", port),
	)
	out, err := cmd.Output()
	if err != nil {
		return
	}
	selfPID := os.Getpid()
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		pid, convErr := strconv.Atoi(line)
		if convErr != nil || pid <= 0 || pid == selfPID {
			continue
		}
		if killErr := exec.Command("taskkill", "/PID", strconv.Itoa(pid), "/F").Run(); killErr != nil {
			logger.Error("failed to stop occupied port process", "addr", addr, "pid", pid, "error", killErr)
			continue
		}
		logger.Info("stopped occupied port process", "addr", addr, "pid", pid)
	}
}

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		return exec.Command("open", url).Start()
	default:
		return exec.Command("xdg-open", url).Start()
	}
}

func resolveConfigPath(path string) string {
	if fileExists(path) {
		return path
	}

	candidates := make([]string, 0, 4)
	if path == "" || path == "config.json" {
		candidates = append(candidates,
			filepath.Join("..", "ctp-future-resources", "config", "config.json"),
			filepath.Join("ctp-future-resources", "config", "config.json"),
		)
	}
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(exeDir, "..", "ctp-future-resources", "config", "config.json"),
			filepath.Join(exeDir, "ctp-future-resources", "config", "config.json"),
		)
	}
	for _, candidate := range candidates {
		if fileExists(candidate) {
			return candidate
		}
	}
	return path
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
