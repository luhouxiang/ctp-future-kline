package web

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"ctp-future-kline/internal/appmode"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/quotes"
	"ctp-future-kline/internal/replay"
	"ctp-future-kline/internal/trade"
)

func (s *Server) LoadKlineChunk(ctx context.Context, req replay.KlineStartRequest, afterAdjusted time.Time, limit int) (replay.KlineChunk, error) {
	_ = ctx
	if limit <= 0 {
		limit = 300
	}
	if limit > 300 {
		limit = 300
	}
	logger.Info("kline replay load chunk from realtime database",
		"symbol", req.Symbol,
		"type", req.Type,
		"variety", req.Variety,
		"timeframe", req.Timeframe,
		"after_adjusted_time", afterAdjusted.Format("2006-01-02 15:04:00"),
		"limit", limit,
		"source_data_mode", "realtime",
		"market_database", s.marketDatabaseForMode(appmode.LiveReal),
	)
	resp, err := s.queryRealtime.BarsFrom(req.Symbol, req.Type, req.Variety, req.Timeframe, afterAdjusted, limit)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return replay.KlineChunk{Exhausted: true}, nil
		}
		return replay.KlineChunk{}, err
	}
	out := make([]replay.KlineBar, 0, len(resp.Bars))
	for _, bar := range resp.Bars {
		adjusted := time.Unix(bar.AdjustedTime, 0)
		dataTime := time.Unix(bar.DataTime, 0)
		out = append(out, replay.KlineBar{
			Symbol:       strings.ToLower(strings.TrimSpace(resp.Meta.Symbol)),
			Type:         strings.ToLower(strings.TrimSpace(resp.Meta.Type)),
			Variety:      strings.ToLower(strings.TrimSpace(resp.Meta.Variety)),
			Timeframe:    strings.ToLower(strings.TrimSpace(req.Timeframe)),
			AdjustedTime: adjusted,
			DataTime:     dataTime,
			Open:         bar.Open,
			High:         bar.High,
			Low:          bar.Low,
			Close:        bar.Close,
			Volume:       bar.Volume,
			OpenInterest: bar.OpenInterest,
		})
	}
	logger.Info("kline replay chunk loaded", "symbol", req.Symbol, "timeframe", req.Timeframe, "after_adjusted", afterAdjusted, "rows", len(out), "limit", limit)
	return replay.KlineChunk{Bars: out, Exhausted: len(out) < limit}, nil
}

func (s *Server) ConsumeKlineBar(ctx context.Context, taskID string, req replay.KlineStartRequest, bar replay.KlineBar) error {
	_ = ctx
	sub := quotes.ChartSubscription{
		Symbol:    firstNonEmpty(strings.TrimSpace(bar.Symbol), req.Symbol),
		Type:      firstNonEmpty(strings.TrimSpace(bar.Type), req.Type),
		Variety:   firstNonEmpty(strings.TrimSpace(bar.Variety), req.Variety),
		Timeframe: firstNonEmpty(strings.TrimSpace(bar.Timeframe), req.Timeframe),
		DataMode:  "realtime",
	}
	if s.replaySink != nil {
		if err := s.replaySink.PublishKlineReplayBar(sub, quotes.ReplayKlineBar{
			Symbol:       sub.Symbol,
			Type:         sub.Type,
			Variety:      sub.Variety,
			Exchange:     bar.Exchange,
			Timeframe:    sub.Timeframe,
			AdjustedTime: bar.AdjustedTime,
			DataTime:     bar.DataTime,
			Open:         bar.Open,
			High:         bar.High,
			Low:          bar.Low,
			Close:        bar.Close,
			Volume:       bar.Volume,
			OpenInterest: bar.OpenInterest,
		}); err != nil {
			return err
		}
	}
	if s.tradePaperReplay != nil {
		if err := s.tradePaperReplay.ConsumePaperMarketBar(trade.PaperMarketBar{
			Symbol:       sub.Symbol,
			ExchangeID:   bar.Exchange,
			Timeframe:    sub.Timeframe,
			AdjustedTime: bar.AdjustedTime,
			DataTime:     bar.DataTime,
			Open:         bar.Open,
			High:         bar.High,
			Low:          bar.Low,
			Close:        bar.Close,
		}); err != nil {
			return err
		}
	}
	logger.Debug("kline replay bar consumed", "task_id", taskID, "symbol", sub.Symbol, "timeframe", sub.Timeframe, "adjusted_time", bar.AdjustedTime, "close", bar.Close)
	return nil
}
