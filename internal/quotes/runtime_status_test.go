package quotes

import (
	"math"
	"testing"
	"time"
)

func TestRuntimeStatusCenterTracksDBWindowsAndQueueDepths(t *testing.T) {
	t.Parallel()

	center := NewRuntimeStatusCenter(time.Minute)
	center.MarkPersistLatency("rb2501", 100)
	center.MarkPersistLatency("rb2501", 300)
	center.MarkDBFlush(10, 20, 15, 5)
	center.MarkDBFlush(30, 40, 9, 3)

	snap := center.Snapshot(time.Now())

	if snap.DBFlushRowsLast != 30 {
		t.Fatalf("DBFlushRowsLast = %d, want 30", snap.DBFlushRowsLast)
	}
	if snap.DBFlushMSLast != 40 {
		t.Fatalf("DBFlushMSLast = %v, want 40", snap.DBFlushMSLast)
	}
	if math.Abs(snap.DBFlushRowsAvg1m-20) > 0.001 {
		t.Fatalf("DBFlushRowsAvg1m = %v, want 20", snap.DBFlushRowsAvg1m)
	}
	if math.Abs(snap.DBFlushMSAvg1m-30) > 0.001 {
		t.Fatalf("DBFlushMSAvg1m = %v, want 30", snap.DBFlushMSAvg1m)
	}
	if snap.DBFlushRowsP951m != 30 {
		t.Fatalf("DBFlushRowsP951m = %d, want 30", snap.DBFlushRowsP951m)
	}
	if math.Abs(snap.PersistQueueMSAvg1m-200) > 0.001 {
		t.Fatalf("PersistQueueMSAvg1m = %v, want 200", snap.PersistQueueMSAvg1m)
	}
	if math.Abs(snap.PersistQueueMSP951m-300) > 0.001 {
		t.Fatalf("PersistQueueMSP951m = %v, want 300", snap.PersistQueueMSP951m)
	}
	if snap.DBQueueDepthTotal != 9 {
		t.Fatalf("DBQueueDepthTotal = %d, want 9", snap.DBQueueDepthTotal)
	}
	if snap.DBQueueDepthInflight != 3 {
		t.Fatalf("DBQueueDepthInflight = %d, want 3", snap.DBQueueDepthInflight)
	}
}
