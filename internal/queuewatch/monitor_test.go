package queuewatch

import "testing"

func TestQueueAlertTransitionsAndSummary(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(Config{
		WarnPercent:      60,
		CriticalPercent:  80,
		EmergencyPercent: 95,
		RecoverPercent:   50,
	})
	handle := registry.Register(QueueSpec{
		Name:        "quotes_primary",
		Category:    "quotes",
		Criticality: "critical",
		Capacity:    100,
		LossPolicy:  "spill_to_disk",
	})

	handle.ObserveDepth(10)
	if got := handle.Snapshot().AlertLevel; got != AlertLevelNormal {
		t.Fatalf("alert level at 10%% = %s, want %s", got, AlertLevelNormal)
	}

	handle.ObserveDepth(65)
	if got := handle.Snapshot().AlertLevel; got != AlertLevelWarn {
		t.Fatalf("alert level at 65%% = %s, want %s", got, AlertLevelWarn)
	}

	handle.ObserveDepth(85)
	if got := handle.Snapshot().AlertLevel; got != AlertLevelCritical {
		t.Fatalf("alert level at 85%% = %s, want %s", got, AlertLevelCritical)
	}

	handle.ObserveDepth(96)
	if got := handle.Snapshot().AlertLevel; got != AlertLevelEmergency {
		t.Fatalf("alert level at 96%% = %s, want %s", got, AlertLevelEmergency)
	}

	handle.ObserveDepth(70)
	if got := handle.Snapshot().AlertLevel; got != AlertLevelEmergency {
		t.Fatalf("alert level should stay emergency until recover threshold, got %s", got)
	}

	handle.ObserveDepth(40)
	snap := handle.Snapshot()
	if snap.AlertLevel != AlertLevelNormal {
		t.Fatalf("alert level at 40%% = %s, want %s", snap.AlertLevel, AlertLevelNormal)
	}
	if snap.HighWatermark != 96 {
		t.Fatalf("high watermark = %d, want 96", snap.HighWatermark)
	}

	handle.MarkSpilled(128, 40)
	summary := registry.Snapshot().Summary
	if summary.SpillingQueues != 1 {
		t.Fatalf("spilling queues = %d, want 1", summary.SpillingQueues)
	}
	handle.MarkSpillRecovered(10)
	summary = registry.Snapshot().Summary
	if summary.SpillingQueues != 0 {
		t.Fatalf("spilling queues after recovery = %d, want 0", summary.SpillingQueues)
	}
}
