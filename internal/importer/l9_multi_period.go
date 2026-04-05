package importer

import (
	"database/sql"
	"errors"

	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/mmkline"
	"ctp-future-kline/internal/quotes"
)

func (s *TDXImportSession) aggregateMultiPeriodsAfterImport(db *sql.DB, sharedDB *sql.DB, bar quotes.MinuteBar, isL9 bool) error {
	if !s.opts.EnableMMRebuild {
		return nil
	}
	variety := quotes.NormalizeVariety(bar.Variety)
	instrumentID := bar.InstrumentID

	written, stats, err := mmkline.RebuildAndUpsertWithSessionDB(db, sharedDB, mmkline.RebuildRequest{
		Variety:      variety,
		InstrumentID: instrumentID,
		IsL9:         isL9,
	}, s.opts.AllowSessionInfer)
	if err != nil {
		if errors.Is(err, mmkline.ErrTradingSessionNotReady) {
			logger.Warn("import post process skipped: trading sessions not ready", "variety", variety, "instrument_id", instrumentID, "is_l9", isL9)
			return nil
		}
		return err
	}

	missingBuckets := 0
	for _, st := range stats {
		if st.ActualMinutes < st.ExpectedMinutes {
			missingBuckets += 1
		}
	}

	table := ""
	if isL9 {
		table, _ = mmkline.TableNameForL9MMVariety(variety)
	} else {
		table, _ = mmkline.TableNameForInstrumentMMVariety(variety)
	}
	logger.Info("kline pipeline",
		"stage", "merge_write_summary",
		"variety", variety,
		"instrument_id", instrumentID,
		"is_l9", isL9,
		"table", table,
		"5m_rows", written["5m"],
		"15m_rows", written["15m"],
		"30m_rows", written["30m"],
		"1h_rows", written["1h"],
		"1d_rows", written["1d"],
		"missing_bucket_count", missingBuckets,
	)
	return nil
}

func (s *TDXImportSession) inferTradingSessionsAfterImport(db *sql.DB, sharedDB *sql.DB, bar quotes.MinuteBar, isL9 bool) error {
	variety := quotes.NormalizeVariety(bar.Variety)
	instrumentID := bar.InstrumentID
	return mmkline.InferAndUpsertTradingSessions(sharedDB, db, mmkline.RebuildRequest{
		Variety:      variety,
		InstrumentID: instrumentID,
		IsL9:         isL9,
	})
}
