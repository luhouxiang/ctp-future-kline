package bus_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	"ctp-go-demo/internal/bus"
	_ "modernc.org/sqlite"
)

func TestConsumerStoreMarkIfFirst(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "dedup.db"))
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := bus.NewConsumerStore(db)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}

	first, err := store.MarkIfFirst("c1", "e1")
	if err != nil {
		t.Fatalf("mark first failed: %v", err)
	}
	if !first {
		t.Fatal("first mark should be true")
	}

	first, err = store.MarkIfFirst("c1", "e1")
	if err != nil {
		t.Fatalf("mark second failed: %v", err)
	}
	if first {
		t.Fatal("second mark should be false")
	}

	first, err = store.MarkIfFirst("c2", "e1")
	if err != nil {
		t.Fatalf("mark with another consumer failed: %v", err)
	}
	if !first {
		t.Fatal("same event for another consumer should be first")
	}
}
