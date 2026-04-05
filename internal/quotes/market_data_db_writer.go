// market_data_db_writer.go 负责把聚合后的 bar 批量写入数据库。
// 它将 shard 生成的持久化任务汇总、分批 flush，降低逐条写库的开销并同步持久化延迟指标。
package quotes

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/queuewatch"
)

type dbWriterWorker struct {
	store         *klineStore
	status        *RuntimeStatusCenter
	owner         *dbBatchWriter
	id            int
	in            chan any
	flushBatch    int
	flushInterval time.Duration
	queueHandle   *queuewatch.QueueHandle
	spool         *queuewatch.JSONSpool[persistTask]

	dropCount atomic.Int64

	lastFlushRows int
	lastFlushMS   float64
}

type dbFlushRequest struct {
	done chan error
}

type dbBatchWriter struct {
	minuteWorkers []*dbWriterWorker
	mmWorkers     []*dbWriterWorker

	mmDeferredCh       chan any
	mmDeferredInterval time.Duration
	mmDeferredBatch    int
	mmDeferredQueue    *queuewatch.QueueHandle
	mmDeferredSpool    *queuewatch.JSONSpool[persistTask]

	mu           sync.Mutex
	ensuredMMTbl map[string]struct{}
}

func newDBBatchWriter(store *klineStore, status *RuntimeStatusCenter, workerCount int, queueCap int, flushBatch int, flushInterval time.Duration) *dbBatchWriter {
	if workerCount <= 0 {
		workerCount = 1
	}
	queueCfg := queuewatch.DefaultConfig("")
	registry := (*queuewatch.Registry)(nil)
	if status != nil && status.QueueRegistry() != nil {
		registry = status.QueueRegistry()
		queueCfg = registry.Config()
	}
	if queueCap <= 0 {
		queueCap = queueCfg.PersistCapacity
	}
	if flushBatch <= 0 {
		flushBatch = defaultDBFlushBatch
	}
	if flushInterval <= 0 {
		flushInterval = defaultDBFlushInterval
	}

	minuteCount := workerCount - 1
	if minuteCount <= 0 {
		minuteCount = 1
	}
	mmCount := workerCount - minuteCount
	if mmCount <= 0 {
		mmCount = 1
		if minuteCount > 1 {
			minuteCount--
		}
	}
	workerQueueCap := queueCap/workerCount + 1

	out := &dbBatchWriter{
		minuteWorkers:      make([]*dbWriterWorker, 0, minuteCount),
		mmWorkers:          make([]*dbWriterWorker, 0, mmCount),
		mmDeferredCh:       make(chan any, queueCfg.MMDeferredCapacity),
		mmDeferredInterval: time.Second,
		mmDeferredBatch:    256,
		ensuredMMTbl:       make(map[string]struct{}),
	}
	if registry != nil {
		out.mmDeferredQueue = registry.Register(queuewatch.QueueSpec{
			Name:        "db_mm_deferred",
			Category:    "quotes_primary",
			Criticality: "critical",
			Capacity:    queueCfg.MMDeferredCapacity,
			LossPolicy:  "spill_to_disk",
			BasisText:   fmt.Sprintf("mm/L9 延迟去重队列一个共享通道，容量等于 mm_deferred_capacity=%d。", queueCfg.MMDeferredCapacity),
		})
		spool, err := queuewatch.NewJSONSpool[persistTask](queueCfg.SpoolDir, out.mmDeferredQueue.Name())
		if err != nil {
			logger.Error("init mm deferred spool failed", "error", err)
		} else {
			out.mmDeferredSpool = spool
			out.mmDeferredQueue.ObserveDepth(len(out.mmDeferredCh) + spool.Pending())
		}
	}
	for i := 0; i < minuteCount; i++ {
		worker := &dbWriterWorker{
			store:         store,
			status:        status,
			owner:         out,
			id:            i,
			in:            make(chan any, workerQueueCap),
			flushBatch:    flushBatch,
			flushInterval: flushInterval,
		}
		if registry != nil {
			worker.queueHandle = registry.Register(queuewatch.QueueSpec{
				Name:        fmt.Sprintf("db_minute_worker_%02d", i),
				Category:    "quotes_primary",
				Criticality: "critical",
				Capacity:    workerQueueCap,
				LossPolicy:  "spill_to_disk",
				BasisText:   fmt.Sprintf("合约 1m DB worker 每个 worker 一个内存队列，容量按 persist_capacity 均分后约为 %d。", workerQueueCap),
			})
			spool, err := queuewatch.NewJSONSpool[persistTask](queueCfg.SpoolDir, worker.queueHandle.Name())
			if err != nil {
				logger.Error("init minute worker spool failed", "worker_id", i, "error", err)
			} else {
				worker.spool = spool
				worker.queueHandle.ObserveDepth(len(worker.in) + spool.Pending())
			}
		}
		out.minuteWorkers = append(out.minuteWorkers, worker)
		go worker.run()
	}
	for i := 0; i < mmCount; i++ {
		workerID := minuteCount + i
		worker := &dbWriterWorker{
			store:         store,
			status:        status,
			owner:         out,
			id:            workerID,
			in:            make(chan any, workerQueueCap),
			flushBatch:    flushBatch,
			flushInterval: flushInterval,
		}
		if registry != nil {
			worker.queueHandle = registry.Register(queuewatch.QueueSpec{
				Name:        fmt.Sprintf("db_mm_worker_%02d", workerID),
				Category:    "quotes_primary",
				Criticality: "critical",
				Capacity:    workerQueueCap,
				LossPolicy:  "spill_to_disk",
				BasisText:   fmt.Sprintf("mm/L9 DB worker 每个 worker 一个内存队列，容量按 persist_capacity 均分后约为 %d。", workerQueueCap),
			})
			spool, err := queuewatch.NewJSONSpool[persistTask](queueCfg.SpoolDir, worker.queueHandle.Name())
			if err != nil {
				logger.Error("init mm worker spool failed", "worker_id", workerID, "error", err)
			} else {
				worker.spool = spool
				worker.queueHandle.ObserveDepth(len(worker.in) + spool.Pending())
			}
		}
		out.mmWorkers = append(out.mmWorkers, worker)
		go worker.run()
	}
	go out.runMMDeferred()
	return out
}

func (w *dbBatchWriter) Enqueue(task persistTask) {
	if w == nil {
		return
	}
	if task.IsL9 || isMMTableName(task.TableName) {
		select {
		case w.mmDeferredCh <- task:
			depth := len(w.mmDeferredCh)
			if w.mmDeferredSpool != nil {
				depth += w.mmDeferredSpool.Pending()
			}
			if w.mmDeferredQueue != nil {
				w.mmDeferredQueue.MarkEnqueued(depth)
			}
		default:
			if w.mmDeferredSpool != nil {
				if size, err := w.mmDeferredSpool.Enqueue(task); err == nil {
					depth := len(w.mmDeferredCh) + w.mmDeferredSpool.Pending()
					if w.mmDeferredQueue != nil {
						w.mmDeferredQueue.MarkSpilled(size, depth)
					}
					return
				}
			}
			logger.Warn("mm/l9 deferred queue full and spill unavailable, blocking enqueue", "table", task.TableName, "instrument_id", task.InstrumentID)
			w.mmDeferredCh <- task
			depth := len(w.mmDeferredCh)
			if w.mmDeferredSpool != nil {
				depth += w.mmDeferredSpool.Pending()
			}
			if w.mmDeferredQueue != nil {
				w.mmDeferredQueue.MarkEnqueued(depth)
			}
		}
		return
	}
	w.enqueueToWorkers(w.minuteWorkers, task)
}

func (w *dbBatchWriter) enqueueToWorkers(workers []*dbWriterWorker, task persistTask) {
	if len(workers) == 0 {
		return
	}
	worker := workers[persistWorkerIndex(task.TableName, len(workers))]
	select {
	case worker.in <- task:
		depth := len(worker.in)
		if worker.spool != nil {
			depth += worker.spool.Pending()
		}
		if worker.queueHandle != nil {
			worker.queueHandle.MarkEnqueued(depth)
		}
	default:
		if worker.spool != nil {
			if size, err := worker.spool.Enqueue(task); err == nil {
				depth := len(worker.in) + worker.spool.Pending()
				if worker.queueHandle != nil {
					worker.queueHandle.MarkSpilled(size, depth)
				}
				return
			}
		}
		worker.in <- task
		depth := len(worker.in)
		if worker.spool != nil {
			depth += worker.spool.Pending()
		}
		if worker.queueHandle != nil {
			worker.queueHandle.MarkEnqueued(depth)
		}
		logger.Warn("db persist queue full and spill unavailable, blocking task enqueue", "table", task.TableName, "instrument_id", task.InstrumentID, "worker_id", worker.id)
	}
}

func (w *dbBatchWriter) Flush() error {
	if w == nil {
		return nil
	}
	if err := w.flushMMDeferred(); err != nil {
		return err
	}
	var firstErr error
	for _, worker := range w.allWorkers() {
		done := make(chan error, 1)
		worker.in <- dbFlushRequest{done: done}
		if err := <-done; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (w *dbBatchWriter) Close() error {
	return w.Flush()
}

func (w *dbBatchWriter) QueueDepth() int {
	if w == nil {
		return 0
	}
	total := len(w.mmDeferredCh)
	if w.mmDeferredSpool != nil {
		total += w.mmDeferredSpool.Pending()
	}
	for _, worker := range w.allWorkers() {
		total += len(worker.in)
		if worker.spool != nil {
			total += worker.spool.Pending()
		}
	}
	return total
}

func (w *dbBatchWriter) DropCount() int64 {
	if w == nil {
		return 0
	}
	var total int64
	for _, worker := range w.allWorkers() {
		total += worker.dropCount.Load()
	}
	return total
}

type mmDeferredFlushRequest struct {
	done chan error
}

func (w *dbBatchWriter) runMMDeferred() {
	ticker := time.NewTicker(w.mmDeferredInterval)
	defer ticker.Stop()

	pending := make(map[string]persistTask)
	for {
		select {
		case msg := <-w.mmDeferredCh:
			depth := len(w.mmDeferredCh)
			if w.mmDeferredSpool != nil {
				depth += w.mmDeferredSpool.Pending()
			}
			if w.mmDeferredQueue != nil {
				w.mmDeferredQueue.MarkDequeued(depth)
			}
			switch v := msg.(type) {
			case persistTask:
				pending[deferredTaskKey(v)] = v
				if len(pending) >= w.mmDeferredBatch {
					w.dispatchDeferredPending(pending)
					pending = make(map[string]persistTask)
				}
			case mmDeferredFlushRequest:
				w.dispatchDeferredPending(pending)
				pending = make(map[string]persistTask)
				v.done <- nil
			}
		default:
			if w.mmDeferredSpool != nil {
				if task, ok, _, err := w.mmDeferredSpool.Dequeue(); err == nil && ok {
					depth := len(w.mmDeferredCh) + w.mmDeferredSpool.Pending()
					if w.mmDeferredQueue != nil {
						w.mmDeferredQueue.MarkSpillRecovered(depth)
						w.mmDeferredQueue.MarkDequeued(depth)
					}
					pending[deferredTaskKey(task)] = task
					if len(pending) >= w.mmDeferredBatch {
						w.dispatchDeferredPending(pending)
						pending = make(map[string]persistTask)
					}
					continue
				} else if err != nil {
					logger.Error("dequeue mm deferred spool failed", "error", err)
				}
			}
		}
		select {
		case <-ticker.C:
			w.dispatchDeferredPending(pending)
			pending = make(map[string]persistTask)
		default:
			if len(pending) == 0 {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
}

func (w *dbBatchWriter) dispatchDeferredPending(pending map[string]persistTask) {
	if len(pending) == 0 {
		return
	}
	tasks := make([]persistTask, 0, len(pending))
	for _, task := range pending {
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].TableName == tasks[j].TableName {
			if tasks[i].Bar.Period == tasks[j].Bar.Period {
				return chooseAdjustedTime(tasks[i].Bar).Before(chooseAdjustedTime(tasks[j].Bar))
			}
			return tasks[i].Bar.Period < tasks[j].Bar.Period
		}
		return tasks[i].TableName < tasks[j].TableName
	})
	for _, task := range tasks {
		w.enqueueToWorkers(w.mmWorkers, task)
	}
}

func (w *dbBatchWriter) flushMMDeferred() error {
	done := make(chan error, 1)
	w.mmDeferredCh <- mmDeferredFlushRequest{done: done}
	return <-done
}

func (w *dbBatchWriter) allWorkers() []*dbWriterWorker {
	if w == nil {
		return nil
	}
	out := make([]*dbWriterWorker, 0, len(w.minuteWorkers)+len(w.mmWorkers))
	out = append(out, w.minuteWorkers...)
	out = append(out, w.mmWorkers...)
	return out
}

func (w *dbWriterWorker) run() {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	buffer := make([]persistTask, 0, w.flushBatch*2)
	for {
		select {
		case msg := <-w.in:
			depth := len(w.in)
			if w.spool != nil {
				depth += w.spool.Pending()
			}
			if w.queueHandle != nil {
				w.queueHandle.MarkDequeued(depth)
			}
			switch v := msg.(type) {
			case persistTask:
				buffer = append(buffer, v)
				if len(buffer) >= w.flushBatch {
					buffer = w.flush(buffer)
					buffer = buffer[:0]
				}
			case dbFlushRequest:
				if len(buffer) > 0 {
					buffer = w.flush(buffer)
					buffer = buffer[:0]
				}
				v.done <- nil
			}
		default:
			if w.spool != nil {
				if task, ok, _, err := w.spool.Dequeue(); err == nil && ok {
					depth := len(w.in) + w.spool.Pending()
					if w.queueHandle != nil {
						w.queueHandle.MarkSpillRecovered(depth)
						w.queueHandle.MarkDequeued(depth)
					}
					buffer = append(buffer, task)
					if len(buffer) >= w.flushBatch {
						buffer = w.flush(buffer)
						buffer = buffer[:0]
					}
					continue
				} else if err != nil {
					logger.Error("dequeue db spool failed", "worker_id", w.id, "error", err)
				}
			}
		}
		select {
		case <-ticker.C:
			if len(buffer) > 0 {
				buffer = w.flush(buffer)
				buffer = buffer[:0]
			}
		default:
			if len(buffer) == 0 {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
}

func (w *dbWriterWorker) flush(buffer []persistTask) []persistTask {
	if len(buffer) == 0 {
		return buffer[:0]
	}
	startedAt := time.Now()
	byTable := make(map[string][]persistTask)
	for _, task := range buffer {
		byTable[task.TableName] = append(byTable[task.TableName], task)
	}
	tables := make([]string, 0, len(byTable))
	for table := range byTable {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	totalRows := 0
	executedStatements := 0
	tableSummaries := make([]string, 0, len(tables))
	for _, table := range tables {
		batch := byTable[table]
		if err := w.upsertBatch(table, batch); err != nil {
			logger.Error("batch upsert failed", "table", table, "worker_id", w.id, "error", err)
			continue
		}
		executedStatements++
		totalRows += len(batch)
		tableSummaries = append(tableSummaries, fmt.Sprintf("%s:%d", table, len(batch)))
		for _, task := range batch {
			endToEndMS := time.Since(task.Trace.ReceivedAt).Seconds() * 1000
			if w.status != nil {
				persistQueueMS := startedAt.Sub(task.Trace.PersistEnqueuedAt).Seconds() * 1000
				w.status.MarkPersistLatency(task.InstrumentID, persistQueueMS)
				w.status.MarkEndToEndLatency(task.InstrumentID, endToEndMS)
			}
		}
	}
	elapsedMS := time.Since(startedAt).Seconds() * 1000
	w.lastFlushRows = totalRows
	w.lastFlushMS = elapsedMS
	if w.status != nil {
		w.status.MarkDBFlush(totalRows, elapsedMS, w.pendingDepth())
	}
	if elapsedMS >= latencyLogThreshold.Seconds()*1000 {
		logger.Warn(
			"db batch flush latency",
			"worker_id", w.id,
			"rows", totalRows,
			"flush_ms", elapsedMS,
			"statement_count", executedStatements,
			"tables", strings.Join(tableSummaries, ","),
		)
	}
	return buffer[:0]
}

func (w *dbWriterWorker) pendingDepth() int {
	if w == nil {
		return 0
	}
	return len(w.in)
}

func (w *dbWriterWorker) upsertBatch(tableName string, tasks []persistTask) error {
	if len(tasks) == 0 {
		return nil
	}
	if isMMTableName(tableName) {
		if err := w.owner.ensureMMTableCached(w.store.db, tableName); err != nil {
			return err
		}
	} else {
		if err := w.store.ensureTable(tableName); err != nil {
			return err
		}
	}

	stmt, args, err := buildUpsertStatement(tableName, tasks)
	if err != nil {
		return err
	}
	_, err = w.store.db.Exec(stmt, args...)
	if err != nil {
		return fmt.Errorf("exec batch upsert failed: %w", err)
	}
	return nil
}

func buildUpsertStatement(tableName string, tasks []persistTask) (string, []any, error) {
	if len(tasks) == 0 {
		return "", nil, nil
	}
	var b strings.Builder
	b.WriteString(`INSERT INTO "`)
	b.WriteString(tableName)
	b.WriteString(`" ("`)
	b.WriteString(colInstrumentID)
	b.WriteString(`","`)
	b.WriteString(colExchange)
	b.WriteString(`","`)
	b.WriteString(colTime)
	b.WriteString(`","`)
	b.WriteString(colAdjustedTime)
	b.WriteString(`","`)
	b.WriteString(colPeriod)
	b.WriteString(`","`)
	b.WriteString(colOpen)
	b.WriteString(`","`)
	b.WriteString(colHigh)
	b.WriteString(`","`)
	b.WriteString(colLow)
	b.WriteString(`","`)
	b.WriteString(colClose)
	b.WriteString(`","`)
	b.WriteString(colVolume)
	b.WriteString(`","`)
	b.WriteString(colOpenInterest)
	b.WriteString(`","`)
	b.WriteString(colSettlement)
	b.WriteString(`") VALUES `)

	args := make([]any, 0, len(tasks)*12)
	for i, task := range tasks {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?)")
		bar := task.Bar
		storedInstrumentID := normalizeInstrumentIDForTable(bar.InstrumentID, tableName)
		if storedInstrumentID == "" {
			return "", nil, fmt.Errorf("invalid instrument id %q for table %q", bar.InstrumentID, tableName)
		}
		args = append(args,
			storedInstrumentID,
			bar.Exchange,
			bar.MinuteTime.Format("2006-01-02 15:04:00"),
			chooseAdjustedTime(bar).Format("2006-01-02 15:04:00"),
			bar.Period,
			bar.Open,
			bar.High,
			bar.Low,
			bar.Close,
			bar.Volume,
			bar.OpenInterest,
			bar.SettlementPrice,
		)
	}
	b.WriteString(` ON DUPLICATE KEY UPDATE "`)
	b.WriteString(colAdjustedTime)
	b.WriteString(`"=VALUES("`)
	b.WriteString(colAdjustedTime)
	b.WriteString(`"),"`)
	b.WriteString(colOpen)
	b.WriteString(`"=VALUES("`)
	b.WriteString(colOpen)
	b.WriteString(`"),"`)
	b.WriteString(colHigh)
	b.WriteString(`"=VALUES("`)
	b.WriteString(colHigh)
	b.WriteString(`"),"`)
	b.WriteString(colLow)
	b.WriteString(`"=VALUES("`)
	b.WriteString(colLow)
	b.WriteString(`"),"`)
	b.WriteString(colClose)
	b.WriteString(`"=VALUES("`)
	b.WriteString(colClose)
	b.WriteString(`"),"`)
	b.WriteString(colVolume)
	b.WriteString(`"=VALUES("`)
	b.WriteString(colVolume)
	b.WriteString(`"),"`)
	b.WriteString(colOpenInterest)
	b.WriteString(`"=VALUES("`)
	b.WriteString(colOpenInterest)
	b.WriteString(`"),"`)
	b.WriteString(colSettlement)
	b.WriteString(`"=VALUES("`)
	b.WriteString(colSettlement)
	b.WriteString(`")`)
	return b.String(), args, nil
}

func ensureMMTable(db *sql.DB, tableName string) error {
	stmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS "%s" (
  "%s" VARCHAR(32) NOT NULL,
  "%s" VARCHAR(16) NOT NULL,
  "%s" DATETIME NOT NULL,
  "%s" DATETIME NOT NULL,
  "%s" VARCHAR(8) NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" BIGINT NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  PRIMARY KEY ("%s", "%s", "%s", "%s")
)`,
		tableName,
		colInstrumentID,
		colExchange,
		colTime,
		colAdjustedTime,
		colPeriod,
		colOpen,
		colHigh,
		colLow,
		colClose,
		colVolume,
		colOpenInterest,
		colSettlement,
		colTime, colInstrumentID, colExchange, colPeriod,
	)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("create mm table failed: %w", err)
	}
	return nil
}

func (w *dbBatchWriter) ensureMMTableCached(db *sql.DB, tableName string) error {
	w.mu.Lock()
	if _, ok := w.ensuredMMTbl[tableName]; ok {
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()

	if err := ensureMMTable(db, tableName); err != nil {
		return err
	}

	w.mu.Lock()
	w.ensuredMMTbl[tableName] = struct{}{}
	w.mu.Unlock()
	return nil
}

func deferredTaskKey(task persistTask) string {
	return strings.Join([]string{
		task.TableName,
		task.Bar.InstrumentID,
		task.Bar.Period,
		task.Bar.MinuteTime.Format("2006-01-02 15:04:00"),
	}, "|")
}

func isMMTableName(tableName string) bool {
	return strings.HasPrefix(tableName, instrumentMMTablePrefix) || strings.HasPrefix(tableName, l9MMTablePrefix)
}

func persistWorkerIndex(tableName string, workerCount int) int {
	if workerCount <= 1 {
		return 0
	}
	h := fnv32(tableName)
	return int(h % uint32(workerCount))
}

func fnv32(v string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	var h uint32 = offset32
	for i := 0; i < len(v); i++ {
		h ^= uint32(v[i])
		h *= prime32
	}
	return h
}
