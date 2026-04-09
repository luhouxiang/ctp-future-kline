package quotes

import (
	"testing"
	"time"

	"ctp-future-kline/internal/testmysql"
)

func TestDBBatchWriterFlushesDedupedRowsToMySQL(t *testing.T) {
	dbPath := testmysql.NewDatabase(t)
	store, err := newKlineStore(dbPath)
	if err != nil {
		t.Fatalf("newKlineStore() error = %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	status := NewRuntimeStatusCenter(time.Minute)
	writer := newDBBatchWriter(store, status, 2, 128, 64, time.Second, 100*time.Millisecond, 64)

	tableName, err := tableNameForVariety("rb")
	if err != nil {
		t.Fatalf("tableNameForVariety() error = %v", err)
	}
	minute := mustParseDBWriterIntegrationTime(t, "2026-04-09 09:31:00")
	receivedAt := time.Now().Add(-20 * time.Millisecond)
	enqueuedAt := time.Now().Add(-10 * time.Millisecond)

	writer.Enqueue(persistTask{
		TableName:    tableName,
		InstrumentID: "rb2501",
		Bar: minuteBar{
			Variety:          "rb",
			InstrumentID:     "rb2501",
			Exchange:         "SHFE",
			MinuteTime:       minute,
			AdjustedTime:     minute,
			Period:           "1m",
			Open:             100,
			High:             101,
			Low:              99,
			Close:            100,
			Volume:           10,
			OpenInterest:     200,
			SettlementPrice:  99.5,
			SourceReceivedAt: receivedAt,
		},
		Trace: runtimeTrace{
			ReceivedAt:        receivedAt,
			PersistEnqueuedAt: enqueuedAt,
		},
	})
	writer.Enqueue(persistTask{
		TableName:    tableName,
		InstrumentID: "rb2501",
		Bar: minuteBar{
			Variety:          "rb",
			InstrumentID:     "rb2501",
			Exchange:         "SHFE",
			MinuteTime:       minute,
			AdjustedTime:     minute,
			Period:           "1m",
			Open:             100,
			High:             103,
			Low:              98,
			Close:            102,
			Volume:           12,
			OpenInterest:     205,
			SettlementPrice:  100.5,
			SourceReceivedAt: receivedAt,
		},
		Trace: runtimeTrace{
			ReceivedAt:        receivedAt,
			PersistEnqueuedAt: enqueuedAt,
		},
	})
	writer.Enqueue(persistTask{
		TableName:    tableName,
		InstrumentID: "rb2505",
		Bar: minuteBar{
			Variety:          "rb",
			InstrumentID:     "rb2505",
			Exchange:         "SHFE",
			MinuteTime:       minute,
			AdjustedTime:     minute,
			Period:           "1m",
			Open:             200,
			High:             202,
			Low:              199,
			Close:            201,
			Volume:           8,
			OpenInterest:     180,
			SettlementPrice:  200.5,
			SourceReceivedAt: receivedAt,
		},
		Trace: runtimeTrace{
			ReceivedAt:        receivedAt,
			PersistEnqueuedAt: enqueuedAt,
		},
	})

	if err := writer.Flush(); err != nil {
		t.Fatalf("writer.Flush() error = %v", err)
	}

	rows, err := store.db.Query(
		`SELECT "InstrumentID","Close","Volume","OpenInterest","SettlementPrice" FROM "future_kline_instrument_1m_rb" ORDER BY "InstrumentID"`,
	)
	if err != nil {
		t.Fatalf("query written rows failed: %v", err)
	}
	defer rows.Close()

	type row struct {
		instrumentID string
		close        float64
		volume       int64
		openInterest float64
		settlement   float64
	}
	var got []row
	for rows.Next() {
		var item row
		if err := rows.Scan(&item.instrumentID, &item.close, &item.volume, &item.openInterest, &item.settlement); err != nil {
			t.Fatalf("scan written row failed: %v", err)
		}
		got = append(got, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate written rows failed: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("row count = %d, want 2 after dedupe", len(got))
	}
	if got[0].instrumentID != "rb2501" || got[0].close != 102 || got[0].volume != 12 || got[0].openInterest != 205 || got[0].settlement != 100.5 {
		t.Fatalf("first row = %+v, want latest rb2501 values", got[0])
	}
	if got[1].instrumentID != "rb2505" || got[1].close != 201 || got[1].volume != 8 || got[1].openInterest != 180 || got[1].settlement != 200.5 {
		t.Fatalf("second row = %+v, want rb2505 values", got[1])
	}

	snap := status.Snapshot(time.Now())
	if snap.DBFlushRowsLast != 2 {
		t.Fatalf("DBFlushRowsLast = %d, want 2", snap.DBFlushRowsLast)
	}
	if snap.DBFlushRowsP951m != 2 {
		t.Fatalf("DBFlushRowsP951m = %d, want 2", snap.DBFlushRowsP951m)
	}
	if snap.DBQueueDepthTotal != 0 || snap.DBQueueDepthInflight != 0 {
		t.Fatalf("queue depths after flush = total:%d inflight:%d, want both 0", snap.DBQueueDepthTotal, snap.DBQueueDepthInflight)
	}
	if snap.DBFlushMSLast <= 0 {
		t.Fatalf("DBFlushMSLast = %v, want > 0", snap.DBFlushMSLast)
	}
	if snap.PersistQueueMSAvg1m < 0 {
		t.Fatalf("PersistQueueMSAvg1m = %v, want >= 0", snap.PersistQueueMSAvg1m)
	}
}

func mustParseDBWriterIntegrationTime(t *testing.T, text string) time.Time {
	t.Helper()
	v, err := time.ParseInLocation("2006-01-02 15:04:05", text, time.Local)
	if err != nil {
		t.Fatalf("time.ParseInLocation(%q) error = %v", text, err)
	}
	return v
}
