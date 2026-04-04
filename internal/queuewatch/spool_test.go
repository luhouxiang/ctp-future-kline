package queuewatch

import "testing"

func TestJSONSpoolPersistsAndRecoversFIFO(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	spool, err := NewJSONSpool[string](root, "test_queue")
	if err != nil {
		t.Fatalf("NewJSONSpool() error = %v", err)
	}
	if spool == nil {
		t.Fatal("NewJSONSpool() returned nil spool")
	}

	if _, err := spool.Enqueue("first"); err != nil {
		t.Fatalf("enqueue first failed: %v", err)
	}
	if _, err := spool.Enqueue("second"); err != nil {
		t.Fatalf("enqueue second failed: %v", err)
	}
	if got := spool.Pending(); got != 2 {
		t.Fatalf("pending after enqueue = %d, want 2", got)
	}

	reloaded, err := NewJSONSpool[string](root, "test_queue")
	if err != nil {
		t.Fatalf("reload spool failed: %v", err)
	}
	if got := reloaded.Pending(); got != 2 {
		t.Fatalf("pending after reload = %d, want 2", got)
	}

	first, ok, _, err := reloaded.Dequeue()
	if err != nil || !ok {
		t.Fatalf("dequeue first failed: ok=%v err=%v", ok, err)
	}
	second, ok, _, err := reloaded.Dequeue()
	if err != nil || !ok {
		t.Fatalf("dequeue second failed: ok=%v err=%v", ok, err)
	}
	if first != "first" || second != "second" {
		t.Fatalf("dequeue order = %q, %q; want first, second", first, second)
	}
	if got := reloaded.Pending(); got != 0 {
		t.Fatalf("pending after drain = %d, want 0", got)
	}
}
