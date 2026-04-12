// main.go 是整个行情服务器的进程入口。
// 它负责加载配置、初始化日志/数据库/交易日历，并创建 web.Server 作为统一控制面，
// 最终把行情、回放、策略、实盘交易等模块挂到同一个 HTTP 服务下对外暴露。
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
	"time"

	"ctp-future-kline/internal/calendar"
	"ctp-future-kline/internal/config"
	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/quotes"
	"ctp-future-kline/internal/userconfig"
	"ctp-future-kline/internal/version"
	"ctp-future-kline/internal/web"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("main panic recovered", "panic", r, "stack", string(debug.Stack()))
			os.Exit(1)
		}
	}()
	configPath := flag.String("config", filepath.Join("config", "config.json"), "config file path")
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
	if err := logger.SetLevel(cfg.Log.Level); err != nil {
		logger.Error("set log level failed", "level", cfg.Log.Level, "error", err)
	}
	// Ensure the first line in file logs is the backend version.
	logger.Debug("config loaded", "config_path", resolvedConfigPath)
	logger.Info("log files enabled", "base_log_path", logPath, "level", cfg.Log.Level)
	if err := quotes.ArchiveTickFilesOnStartup(cfg.CTP.FlowPath, time.Now()); err != nil {
		logger.Error("archive tick files on startup failed", "flow_path", cfg.CTP.FlowPath, "error", err)
	}
	if err := dbx.EnsureAllLogicalDatabases(cfg.DB); err != nil {
		logger.Error("ensure mysql database failed", "error", err)
	}
	if err := dbx.MigrateSharedMetaTables(cfg.DB); err != nil {
		logger.Error("migrate shared meta tables failed", "error", err)
	}
	dsn := dbx.DSNForRole(cfg.DB, dbx.RoleSharedMeta)
	db, err := dbx.Open(dsn)
	if err != nil {
		logger.Error("open mysql failed", "error", err)
		os.Exit(1)
	}
	if err := dbx.EnsureDatabaseAndSchemaForRole(dbx.ConfigForRole(cfg.DB, dbx.RoleSharedMeta), dbx.RoleSharedMeta, db); err != nil {
		logger.Error("ensure mysql schema failed", "error", err)
	}
	overrideStore := &userconfig.Store{}
	overrideStore, err = userconfig.NewStore(dsn)
	if err != nil {
		logger.Error("open user config store failed", "error", err)
	} else {
		overrides, loadErr := overrideStore.LoadAppOverrides(userconfig.DefaultOwner)
		if loadErr != nil {
			logger.Error("load user config overrides failed", "owner", userconfig.DefaultOwner, "error", loadErr)
		} else {
			cfg = userconfig.ApplyAppOverrides(cfg, overrides)
		}
		_ = overrideStore.Close()
	}
	_ = db.Close()
	cfg.CTP.DBDSN = dbx.DSNForRole(cfg.DB, dbx.RoleMarketRealtime)
	cfg.CTP.SharedMetaDSN = dsn
	cm := calendar.NewManager(dbx.DSNForRole(cfg.DB, dbx.RoleSharedMeta))
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
	killedAny := false
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
		killedAny = true
		logger.Info("stopped occupied port process", "addr", addr, "pid", pid)
	}
	if killedAny {
		time.Sleep(500 * time.Millisecond)
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
	if path == "" || path == "config.json" || path == filepath.Join("config", "config.json") {
		candidates = append(candidates,
			filepath.Join("config", "config.json"),
			"config.json",
			filepath.Join("..", "ctp-future-resources", "config", "config.json"),
			filepath.Join("ctp-future-resources", "config", "config.json"),
		)
	}
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(exeDir, "config", "config.json"),
			filepath.Join(exeDir, "..", "config", "config.json"),
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
