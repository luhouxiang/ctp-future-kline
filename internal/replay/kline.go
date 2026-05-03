package replay

import (
	"context"
	"fmt"
	"time"

	"ctp-future-kline/internal/logger"
)

const (
	defaultKlineChunkSize     = 300
	defaultKlinePrefetchBelow = 50
	maxKlineChunkSize         = 300
)

func (s *Service) runKline(ctx context.Context, taskID string, req StartRequest) {
	if req.Kline == nil {
		s.failTask(taskID, fmt.Errorf("kline replay request is required"))
		return
	}
	handler := s.kline
	if handler == nil {
		s.failTask(taskID, fmt.Errorf("kline replay handler is not registered"))
		return
	}
	kreq := *req.Kline
	kreq.ChunkSize = normalizeKlineChunkSize(kreq.ChunkSize)
	kreq.IntervalMS = normalizeKlineIntervalMS(kreq.IntervalMS)
	queue := make([]KlineBar, 0, kreq.ChunkSize*2)
	sourceExhausted := false
	var lastLoaded time.Time
	if !kreq.AnchorAdjustedTime.IsZero() {
		lastLoaded = kreq.AnchorAdjustedTime.Add(-time.Nanosecond)
	}
	logger.Info("kline replay load initial chunk begin", "task_id", taskID, "symbol", kreq.Symbol, "timeframe", kreq.Timeframe, "anchor_adjusted_time", kreq.AnchorAdjustedTime, "chunk_size", kreq.ChunkSize)
	chunk, err := handler.LoadKlineChunk(ctx, kreq, lastLoaded, kreq.ChunkSize)
	if err != nil {
		s.failTask(taskID, err)
		return
	}
	queue = append(queue, chunk.Bars...)
	sourceExhausted = chunk.Exhausted
	if len(chunk.Bars) > 0 {
		lastLoaded = chunk.Bars[len(chunk.Bars)-1].AdjustedTime
	}
	s.updateKlineLoadStats(taskID, len(queue), sourceExhausted)
	logger.Info("kline replay load initial chunk done", "task_id", taskID, "loaded", len(chunk.Bars), "source_exhausted", sourceExhausted, "last_loaded_adjusted_time", lastLoaded)

	processed := 0
	for {
		if err := s.waitIfPaused(ctx, taskID); err != nil {
			if err != context.Canceled {
				s.failTask(taskID, err)
			}
			return
		}
		if len(queue) == 0 {
			if sourceExhausted {
				logger.Info("kline replay source exhausted", "task_id", taskID, "processed_bars", processed)
				return
			}
			chunk, err := handler.LoadKlineChunk(ctx, kreq, lastLoaded, kreq.ChunkSize)
			if err != nil {
				s.failTask(taskID, err)
				return
			}
			queue = append(queue, chunk.Bars...)
			sourceExhausted = chunk.Exhausted
			if len(chunk.Bars) > 0 {
				lastLoaded = chunk.Bars[len(chunk.Bars)-1].AdjustedTime
			}
			s.updateKlineLoadStats(taskID, processed+len(queue), sourceExhausted)
			if len(queue) == 0 && sourceExhausted {
				logger.Info("kline replay completed without more source rows", "task_id", taskID, "processed_bars", processed)
				return
			}
			continue
		}
		if !sourceExhausted && len(queue) <= defaultKlinePrefetchBelow {
			chunk, err := handler.LoadKlineChunk(ctx, kreq, lastLoaded, kreq.ChunkSize)
			if err != nil {
				s.failTask(taskID, err)
				return
			}
			queue = append(queue, chunk.Bars...)
			sourceExhausted = chunk.Exhausted
			if len(chunk.Bars) > 0 {
				lastLoaded = chunk.Bars[len(chunk.Bars)-1].AdjustedTime
			}
			s.updateKlineLoadStats(taskID, processed+len(queue), sourceExhausted)
			logger.Info("kline replay prefetch done", "task_id", taskID, "loaded", len(chunk.Bars), "queue_remaining", len(queue), "source_exhausted", sourceExhausted, "last_loaded_adjusted_time", lastLoaded)
		}

		bar := queue[0]
		queue = queue[1:]
		if err := handler.ConsumeKlineBar(ctx, taskID, kreq, bar); err != nil {
			s.failTask(taskID, err)
			return
		}
		processed++
		s.updateKlineProgress(taskID, bar, processed)
		if err := s.waitKlineInterval(ctx, taskID, time.Duration(kreq.IntervalMS)*time.Millisecond); err != nil {
			if err != context.Canceled {
				s.failTask(taskID, err)
			}
			return
		}
	}
}

func normalizeKlineChunkSize(n int) int {
	if n <= 0 {
		return defaultKlineChunkSize
	}
	if n > maxKlineChunkSize {
		return maxKlineChunkSize
	}
	return n
}

func (s *Service) updateKlineLoadStats(taskID string, totalLoaded int, exhausted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.TaskID != taskID {
		return
	}
	if int64(totalLoaded) > s.snapshot.TotalTicks {
		s.snapshot.TotalTicks = int64(totalLoaded)
	}
	if exhausted {
		s.snapshot.Instruments = 1
	}
}

func (s *Service) updateKlineProgress(taskID string, bar KlineBar, processed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.TaskID != taskID {
		return
	}
	s.snapshot.ProcessedTicks = int64(processed)
	s.snapshot.Dispatched++
	s.snapshot.CurrentInstrumentID = bar.Symbol
	ts := bar.AdjustedTime
	s.snapshot.CurrentSimTime = &ts
	if s.snapshot.FirstSimTime == nil {
		first := bar.AdjustedTime
		s.snapshot.FirstSimTime = &first
	}
	last := bar.AdjustedTime
	s.snapshot.LastSimTime = &last
}

func (s *Service) waitKlineInterval(ctx context.Context, taskID string, fallback time.Duration) error {
	if fallback <= 0 {
		fallback = time.Second
	}
	const maxSleepSlice = 100 * time.Millisecond
	remaining := fallback
	for remaining > 0 {
		if err := s.waitIfPaused(ctx, taskID); err != nil {
			return err
		}
		interval := time.Duration(s.currentSpeed(taskID, float64(fallback/time.Millisecond))) * time.Millisecond
		if interval <= 0 {
			interval = fallback
		}
		if interval != fallback {
			remaining = interval
			fallback = interval
		}
		wait := remaining
		if wait > maxSleepSlice {
			wait = maxSleepSlice
		}
		startedAt := time.Now()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		remaining -= time.Since(startedAt)
	}
	return nil
}

func (s *Service) failTask(taskID string, err error) {
	if err == nil {
		return
	}
	logger.Info("replay task failed", "task_id", taskID, "error", err)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.TaskID != taskID {
		return
	}
	s.snapshot.Status = StatusError
	s.snapshot.LastError = err.Error()
	s.snapshot.Errors++
	s.snapshot.FinishedAt = time.Now()
}
