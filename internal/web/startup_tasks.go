package web

import (
	"strings"
	"time"
)

const (
	startupTaskPending = "pending"
	startupTaskRunning = "running"
	startupTaskDone    = "done"
	startupTaskSkipped = "skipped"
	startupTaskError   = "error"
)

type startupTaskSnapshot struct {
	Key        string `json:"key"`
	Title      string `json:"title"`
	Status     string `json:"status"`
	Detail     string `json:"detail"`
	Error      string `json:"error,omitempty"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
}

func (s *Server) setStartupTask(key string, title string, status string, detail string, err error) startupTaskSnapshot {
	now := time.Now()
	s.startupMu.Lock()
	if s.startupTasks == nil {
		s.startupTasks = make(map[string]startupTaskSnapshot)
	}
	prev := s.startupTasks[key]
	if strings.TrimSpace(prev.StartedAt) == "" && (status == startupTaskRunning || status == startupTaskDone || status == startupTaskError || status == startupTaskSkipped) {
		prev.StartedAt = now.Format(time.RFC3339)
	}
	prev.Key = key
	prev.Title = title
	prev.Status = status
	prev.Detail = detail
	prev.Error = ""
	if err != nil {
		prev.Error = err.Error()
	}
	if status == startupTaskDone || status == startupTaskError || status == startupTaskSkipped {
		prev.FinishedAt = now.Format(time.RFC3339)
	}
	s.startupTasks[key] = prev
	s.startupMu.Unlock()
	s.broadcastEvent("startup_task_update", prev)
	return prev
}

func (s *Server) startupTaskSnapshots() []startupTaskSnapshot {
	s.startupMu.Lock()
	defer s.startupMu.Unlock()
	out := make([]startupTaskSnapshot, 0, len(s.startupTasks))
	order := []string{"calendar_auto_update", "strategy", "trade_live", "trade_paper_live", "trade_paper_replay"}
	seen := make(map[string]bool, len(s.startupTasks))
	for _, key := range order {
		if item, ok := s.startupTasks[key]; ok {
			out = append(out, item)
			seen[key] = true
		}
	}
	for key, item := range s.startupTasks {
		if !seen[key] {
			out = append(out, item)
		}
	}
	return out
}

func (s *Server) runStartupTask(key string, title string, detail string, fn func() error) {
	s.setStartupTask(key, title, startupTaskRunning, detail, nil)
	go func() {
		if err := fn(); err != nil {
			s.setStartupTask(key, title, startupTaskError, detail, err)
			return
		}
		s.setStartupTask(key, title, startupTaskDone, detail, nil)
	}()
}
