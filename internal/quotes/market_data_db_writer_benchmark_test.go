package quotes

import (
	"fmt"
	"testing"
	"time"

	"ctp-future-kline/internal/testmysql"
)

func BenchmarkDBBatchWriterMySQL(b *testing.B) {
	benchmarks := []struct {
		name         string
		workerCount  int
		flushBatch   int
		duplicateHot bool
	}{
		{name: "writers4_batch512_unique", workerCount: 4, flushBatch: 512, duplicateHot: false},
		{name: "writers8_batch128_unique", workerCount: 8, flushBatch: 128, duplicateHot: false},
		{name: "writers4_batch512_duplicatehot", workerCount: 4, flushBatch: 512, duplicateHot: true},
		{name: "writers8_batch128_duplicatehot", workerCount: 8, flushBatch: 128, duplicateHot: true},
	}

	for _, tc := range benchmarks {
		b.Run(tc.name, func(b *testing.B) {
			dsn := testmysql.NewBenchmarkDatabase(b)
			store, err := newKlineStore(dsn)
			if err != nil {
				b.Fatalf("newKlineStore() error = %v", err)
			}
			b.Cleanup(func() {
				_ = store.Close()
			})

			writer := newDBBatchWriter(store, nil, tc.workerCount, 16384, tc.flushBatch, 30*time.Millisecond, 100*time.Millisecond, 512)
			b.Cleanup(func() {
				_ = writer.Close()
			})

			tableName, err := tableNameForVariety("rb")
			if err != nil {
				b.Fatalf("tableNameForVariety() error = %v", err)
			}
			batchSize := tc.flushBatch
			if batchSize < 64 {
				batchSize = 64
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tasks := buildBenchmarkPersistTasks(tableName, i, batchSize, tc.duplicateHot)
				for _, task := range tasks {
					writer.Enqueue(task)
				}
				if err := writer.Flush(); err != nil {
					b.Fatalf("writer.Flush() error = %v", err)
				}
			}
		})
	}
}

func buildBenchmarkPersistTasks(tableName string, iter int, batchSize int, duplicateHot bool) []persistTask {
	tasks := make([]persistTask, 0, batchSize)
	baseMinute := time.Date(2026, 4, 10, 9, 0, 0, 0, time.Local).Add(time.Duration(iter) * time.Minute)
	now := time.Now()
	uniqueKeys := batchSize
	if duplicateHot {
		uniqueKeys = batchSize / 4
		if uniqueKeys <= 0 {
			uniqueKeys = 1
		}
	}
	for i := 0; i < batchSize; i++ {
		keyIndex := i
		if duplicateHot {
			keyIndex = i % uniqueKeys
		}
		instrumentID := fmt.Sprintf("rb%04d", 2501+keyIndex)
		minute := baseMinute.Add(time.Duration(keyIndex%8) * time.Minute)
		closePrice := float64(100 + keyIndex + i%3)
		tasks = append(tasks, persistTask{
			TableName:    tableName,
			InstrumentID: instrumentID,
			Bar: minuteBar{
				Variety:          "rb",
				InstrumentID:     instrumentID,
				Exchange:         "SHFE",
				MinuteTime:       minute,
				AdjustedTime:     minute,
				Period:           "1m",
				Open:             closePrice - 1,
				High:             closePrice + 1,
				Low:              closePrice - 2,
				Close:            closePrice,
				Volume:           int64(10 + i),
				OpenInterest:     float64(100 + keyIndex),
				SettlementPrice:  closePrice - 0.5,
				SourceReceivedAt: now,
			},
			Trace: runtimeTrace{
				ReceivedAt:        now,
				PersistEnqueuedAt: now,
			},
		})
	}
	return tasks
}
