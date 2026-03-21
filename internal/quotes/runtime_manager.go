// runtime_manager.go 封装行情主链路的生命周期入口。
// 它负责启动 quotes.Service，捕获顶层 panic/error，并把运行状态同步到 RuntimeStatusCenter。
package quotes

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/logger"
)

type RuntimeManager struct {
	// cfg 保存行情主链路运行所需的 CTP 配置。
	cfg config.CTPConfig
	// status 指向全局行情状态中心，用于同步启动和运行结果。
	status *RuntimeStatusCenter

	// mu 保护 started 状态，避免重复启动行情主链路。
	mu sync.Mutex
	// started 标记是否已经触发过 Start。
	started bool
}

func NewRuntimeManager(cfg config.CTPConfig, status *RuntimeStatusCenter) *RuntimeManager {
	return &RuntimeManager{cfg: cfg, status: status}
}

func (m *RuntimeManager) Start() error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return nil
	}
	m.started = true
	m.mu.Unlock()

	m.status.SetStarting()
	go m.runLoop()
	return nil
}

func (m *RuntimeManager) runLoop() {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("runtime manager panic: %v", r)
			logger.Error("runtime manager panic recovered", "panic", r, "stack", string(debug.Stack()))
			m.status.SetError(err)
		}
	}()
	svc := NewService(m.cfg)
	if err := svc.RunContinuous(m.status); err != nil {
		logger.Error("runtime manager loop failed", "error", err)
		m.status.SetError(err)
		return
	}
	m.status.SetError(fmt.Errorf("runtime manager exited unexpectedly at %s", time.Now().Format(time.RFC3339)))
}
