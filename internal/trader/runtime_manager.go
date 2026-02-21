package trader

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"ctp-go-demo/internal/config"
	"ctp-go-demo/internal/logger"
)

type RuntimeManager struct {
	cfg    config.CTPConfig
	status *RuntimeStatusCenter

	mu      sync.Mutex
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
