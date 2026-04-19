package trade

import (
	"strings"
	"time"
)

func (s *Service) startPaperMetaSync() error {
	if !s.livePaper || s.tradeOpGateway == nil || s.rateCatalog == nil {
		return nil
	}
	if err := s.tradeOpGateway.Start(); err != nil {
		return err
	}
	s.RequestMetaSync()
	go s.runPaperMetaSyncLoop()
	return nil
}

func (s *Service) runPaperMetaSyncLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		day := strings.TrimSpace(s.tradeOpGateway.Status().TradingDay)
		if day != "" {
			s.setStatus(func(st *TradeStatus) { st.TradingDay = day })
		}
		s.metaSyncMu.Lock()
		requested := s.metaSyncRequested
		dayChanged := day != "" && day != s.metaSyncTradingDay
		if !requested && !dayChanged {
			s.metaSyncMu.Unlock()
			continue
		}
		if s.metaSyncRunning {
			s.metaSyncMu.Unlock()
			continue
		}
		s.metaSyncRunning = true
		s.metaSyncRequested = false
		s.metaSyncMu.Unlock()
		s.runPaperMetaSyncOnce(day)
		s.metaSyncMu.Lock()
		s.metaSyncRunning = false
		if day != "" {
			s.metaSyncTradingDay = day
		}
		s.metaSyncMu.Unlock()
	}
}

func (s *Service) runPaperMetaSyncOnce(tradingDay string) {
	if strings.TrimSpace(tradingDay) == "" || s.rateCatalog == nil {
		return
	}
	feeMissing, err := s.buildMissingCommissionQueue(tradingDay)
	if err != nil {
		s.logLaneError(newLaneThrottle("fee_lane", "commission_rate", s.cfg), err, "", "")
		return
	}
	marginMissing, err := s.buildMissingMarginQueue(tradingDay)
	if err != nil {
		s.logLaneError(newLaneThrottle("margin_lane", "margin_rate", s.cfg), err, "", "")
		return
	}
	s.setStatus(func(st *TradeStatus) {
		st.MetaSyncTradingDay = tradingDay
		st.FeeGap = len(feeMissing)
		st.MarginGap = len(marginMissing)
		st.FeeThrottleMS = s.feeThrottle.interval.Milliseconds()
		st.MarginThrottleMS = s.marginThrottle.interval.Milliseconds()
	})

	if len(feeMissing) > 0 && s.feeGateway != nil {
		if err := s.feeGateway.Start(); err != nil {
			s.logLaneError(s.feeThrottle, err, "", "")
		} else {
			s.setStatus(func(st *TradeStatus) { st.FeeLaneRunning = true })
			for len(feeMissing) > 0 {
				select {
				case <-s.ctx.Done():
					return
				default:
				}
				item := feeMissing[0]
				rates, qerr := s.feeGateway.QueryCommissionRate(item.InstrumentID, item.ExchangeID)
				s.auditQuery("commission_rate", qerr)
				if qerr != nil {
					s.logLaneError(s.feeThrottle, qerr, item.InstrumentID, item.ExchangeID)
					s.feeThrottle.onFlowControl(qerr, item.InstrumentID, item.ExchangeID)
					s.setStatus(func(st *TradeStatus) {
						st.FeeThrottleMS = s.feeThrottle.interval.Milliseconds()
						st.LastError = qerr.Error()
					})
				} else {
					if _, upErr := s.rateCatalog.upsertCommissionRates(tradingDay, rates); upErr != nil {
						s.logLaneError(s.feeThrottle, upErr, item.InstrumentID, item.ExchangeID)
					} else {
						feeMissing = feeMissing[1:]
						s.setStatus(func(st *TradeStatus) { st.FeeGap = len(feeMissing) })
					}
				}
				if !sleepOrDone(s.ctx, s.feeThrottle.interval) {
					return
				}
			}
			s.setStatus(func(st *TradeStatus) { st.FeeLaneRunning = false })
		}
	}

	if len(marginMissing) > 0 && s.marginGateway != nil {
		if err := s.marginGateway.Start(); err != nil {
			s.logLaneError(s.marginThrottle, err, "", "")
		} else {
			s.setStatus(func(st *TradeStatus) { st.MarginLaneRunning = true })
			for len(marginMissing) > 0 {
				select {
				case <-s.ctx.Done():
					return
				default:
				}
				item := marginMissing[0]
				rates, qerr := s.marginGateway.QueryMarginRate(item.InstrumentID, item.ExchangeID)
				s.auditQuery("margin_rate", qerr)
				if qerr != nil {
					s.logLaneError(s.marginThrottle, qerr, item.InstrumentID, item.ExchangeID)
					s.marginThrottle.onFlowControl(qerr, item.InstrumentID, item.ExchangeID)
					s.setStatus(func(st *TradeStatus) {
						st.MarginThrottleMS = s.marginThrottle.interval.Milliseconds()
						st.LastError = qerr.Error()
					})
				} else {
					if _, upErr := s.rateCatalog.upsertMarginRates(tradingDay, rates); upErr != nil {
						s.logLaneError(s.marginThrottle, upErr, item.InstrumentID, item.ExchangeID)
					} else {
						marginMissing = marginMissing[1:]
						s.setStatus(func(st *TradeStatus) { st.MarginGap = len(marginMissing) })
					}
				}
				if !sleepOrDone(s.ctx, s.marginThrottle.interval) {
					return
				}
			}
			s.setStatus(func(st *TradeStatus) { st.MarginLaneRunning = false })
		}
	}
}
