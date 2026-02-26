package importer

import (
	"database/sql"
	"errors"
	"fmt"

	"ctp-go-demo/internal/logger"
	"ctp-go-demo/internal/mmkline"
	"ctp-go-demo/internal/trader"
)

func (s *TDXImportSession) aggregateMultiPeriodsAfterImport(db *sql.DB, bar trader.MinuteBar, isL9 bool) error {
	variety := trader.NormalizeVariety(bar.Variety)
	instrumentID := bar.InstrumentID

	written, stats, err := mmkline.RebuildAndUpsert(db, mmkline.RebuildRequest{
		Variety:      variety,
		InstrumentID: instrumentID,
		IsL9:         isL9,
	})
	if err != nil {
		if errors.Is(err, mmkline.ErrTradingSessionNotReady) {
			return fmt.Errorf("%w: variety=%s", mmkline.ErrTradingSessionNotReady, variety)
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
