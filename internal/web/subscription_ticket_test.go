package web

import (
	"testing"
	"time"
)

func TestSubscriptionTicketManagerReuseAndRotate(t *testing.T) {
	mgr := newSubscriptionTicketManager("")
	now := time.Date(2026, 4, 18, 9, 30, 0, 0, time.Local)
	mgr.nowFunc = func() time.Time { return now }

	sub := map[string]any{
		"symbol":    "ag2605",
		"type":      "contract",
		"variety":   "ag",
		"timeframe": "1m",
		"data_mode": "replay",
	}

	a := mgr.Resolve("admin", sub, "ag", "replay_paper")
	if a.TicketID == "" {
		t.Fatal("ticket_id should not be empty")
	}
	if len(a.Sessions) == 0 {
		t.Fatal("sessions should not be empty")
	}
	b := mgr.Resolve("admin", sub, "ag", "replay_paper")
	if a.TicketID != b.TicketID {
		t.Fatalf("ticket should be reused for same key/version, got %q vs %q", a.TicketID, b.TicketID)
	}

	now = now.Add(24 * time.Hour)
	c := mgr.Resolve("admin", sub, "ag", "replay_paper")
	if c.TicketID == a.TicketID {
		t.Fatalf("ticket should rotate after day switch, still %q", c.TicketID)
	}
}
