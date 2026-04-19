package trade

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	dbx "ctp-future-kline/internal/db"
)

type instrumentKey struct {
	InstrumentID string
	ExchangeID   string
}

type rateCatalog struct {
	db *sql.DB
}

func newRateCatalog(sharedDSN string) (*rateCatalog, error) {
	sharedDSN = strings.TrimSpace(sharedDSN)
	if sharedDSN == "" {
		return nil, fmt.Errorf("shared dsn is empty")
	}
	db, err := dbx.Open(sharedDSN)
	if err != nil {
		return nil, err
	}
	return &rateCatalog{db: db}, nil
}

func (c *rateCatalog) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

func (c *rateCatalog) listInstrumentsByTradingDay(tradingDay string) ([]instrumentKey, error) {
	if c == nil || c.db == nil {
		return nil, nil
	}
	rows, err := c.db.Query(
		`SELECT instrument_id,exchange_id
FROM ctp_instruments
WHERE trading_day=? OR sync_trading_day=?
ORDER BY exchange_id ASC, instrument_id ASC`,
		strings.TrimSpace(tradingDay),
		strings.TrimSpace(tradingDay),
	)
	if err != nil {
		return nil, fmt.Errorf("query instruments by trading day failed: %w", err)
	}
	defer rows.Close()
	out := make([]instrumentKey, 0, 1024)
	seen := make(map[string]struct{}, 1024)
	for rows.Next() {
		var item instrumentKey
		if err := rows.Scan(&item.InstrumentID, &item.ExchangeID); err != nil {
			return nil, fmt.Errorf("scan instruments by trading day failed: %w", err)
		}
		k := strings.ToLower(strings.TrimSpace(item.InstrumentID)) + "|" + strings.TrimSpace(item.ExchangeID)
		if k == "|" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate instruments by trading day failed: %w", err)
	}
	return out, nil
}

func (c *rateCatalog) missingCommissionInstruments(tradingDay string, all []instrumentKey) ([]instrumentKey, error) {
	return c.missingByTable(tradingDay, all, `SELECT instrument_id,exchange_id FROM ctp_commission_rates WHERE sync_trading_day=?`)
}

func (c *rateCatalog) missingMarginInstruments(tradingDay string, all []instrumentKey) ([]instrumentKey, error) {
	return c.missingByTable(tradingDay, all, `SELECT DISTINCT instrument_id,exchange_id FROM ctp_margin_rates WHERE sync_trading_day=?`)
}

func (c *rateCatalog) missingByTable(tradingDay string, all []instrumentKey, presentSQL string) ([]instrumentKey, error) {
	if c == nil || c.db == nil {
		return nil, nil
	}
	rows, err := c.db.Query(presentSQL, strings.TrimSpace(tradingDay))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	present := make(map[string]struct{}, len(all))
	for rows.Next() {
		var instrumentID, exchangeID string
		if err := rows.Scan(&instrumentID, &exchangeID); err != nil {
			return nil, err
		}
		k := strings.ToLower(strings.TrimSpace(instrumentID)) + "|" + strings.TrimSpace(exchangeID)
		if k != "|" {
			present[k] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]instrumentKey, 0, len(all))
	for _, item := range all {
		k := strings.ToLower(strings.TrimSpace(item.InstrumentID)) + "|" + strings.TrimSpace(item.ExchangeID)
		if k == "|" {
			continue
		}
		if _, ok := present[k]; ok {
			continue
		}
		out = append(out, item)
	}
	return out, nil
}

func (c *rateCatalog) upsertCommissionRates(tradingDay string, items []CommissionRateSnapshot) (int, error) {
	if c == nil || c.db == nil || len(items) == 0 {
		return 0, nil
	}
	tx, err := c.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(
		`REPLACE INTO ctp_commission_rates(
instrument_id,exchange_id,open_ratio_by_money,open_ratio_by_volume,
close_ratio_by_money,close_ratio_by_volume,close_today_ratio_by_money,close_today_ratio_by_volume,
sync_trading_day,updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?)`,
	)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	now := time.Now()
	n := 0
	for _, item := range items {
		if strings.TrimSpace(item.InstrumentID) == "" {
			continue
		}
		if _, err = stmt.Exec(
			item.InstrumentID,
			item.ExchangeID,
			item.OpenRatioByMoney,
			item.OpenRatioByVolume,
			item.CloseRatioByMoney,
			item.CloseRatioByVolume,
			item.CloseTodayRatioByMoney,
			item.CloseTodayRatioByVolume,
			strings.TrimSpace(tradingDay),
			now,
		); err != nil {
			return 0, err
		}
		n++
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

func (c *rateCatalog) upsertMarginRates(tradingDay string, items []MarginRateSnapshot) (int, error) {
	if c == nil || c.db == nil || len(items) == 0 {
		return 0, nil
	}
	tx, err := c.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(
		`REPLACE INTO ctp_margin_rates(
instrument_id,exchange_id,hedge_flag,long_margin_ratio_by_money,long_margin_ratio_by_volume,
short_margin_ratio_by_money,short_margin_ratio_by_volume,is_relative,sync_trading_day,updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?)`,
	)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	now := time.Now()
	n := 0
	for _, item := range items {
		if strings.TrimSpace(item.InstrumentID) == "" {
			continue
		}
		hedgeFlag := strings.TrimSpace(item.HedgeFlag)
		if hedgeFlag == "" {
			hedgeFlag = "speculation"
		}
		isRelative := 0
		if item.IsRelative {
			isRelative = 1
		}
		if _, err = stmt.Exec(
			item.InstrumentID,
			item.ExchangeID,
			hedgeFlag,
			item.LongMarginRatioByMoney,
			item.LongMarginRatioByVolume,
			item.ShortMarginRatioByMoney,
			item.ShortMarginRatioByVolume,
			isRelative,
			strings.TrimSpace(tradingDay),
			now,
		); err != nil {
			return 0, err
		}
		n++
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}
