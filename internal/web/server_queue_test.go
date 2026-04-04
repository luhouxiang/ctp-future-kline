package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ctp-future-kline/internal/queuewatch"
	"ctp-future-kline/internal/quotes"
)

func TestHandleQueuesReturnsQueueSnapshot(t *testing.T) {
	t.Parallel()

	status := quotes.NewRuntimeStatusCenter(time.Minute)
	registry := status.QueueRegistry()
	handle := registry.Register(queuewatch.QueueSpec{
		Name:        "test_queue",
		Category:    "quotes_primary",
		Criticality: "critical",
		Capacity:    16,
		LossPolicy:  "spill_to_disk",
		BasisText:   "test",
	})
	handle.ObserveDepth(7)

	s := &Server{status: status}
	req := httptest.NewRequest(http.MethodGet, "/api/queues", nil)
	rec := httptest.NewRecorder()

	s.handleQueues(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Summary struct {
			TotalQueues int `json:"total_queues"`
		} `json:"summary"`
		Queues []struct {
			Name         string  `json:"name"`
			CurrentDepth int     `json:"current_depth"`
			UsagePercent float64 `json:"usage_percent"`
		} `json:"queues"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.Summary.TotalQueues == 0 {
		t.Fatal("expected queue summary to include at least one queue")
	}
	if len(resp.Queues) == 0 {
		t.Fatal("expected queue list to be non-empty")
	}
	var found bool
	for _, item := range resp.Queues {
		if item.Name != "test_queue" {
			continue
		}
		found = true
		if item.CurrentDepth != 7 {
			t.Fatalf("current depth = %d, want 7", item.CurrentDepth)
		}
		if item.UsagePercent <= 0 {
			t.Fatalf("usage percent = %v, want > 0", item.UsagePercent)
		}
	}
	if !found {
		t.Fatal("test_queue not found in queue snapshot")
	}
}
