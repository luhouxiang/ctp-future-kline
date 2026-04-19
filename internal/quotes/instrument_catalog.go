package quotes

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"ctp-future-kline/internal/logger"
)

const (
	ctpInstrumentTableName    = "ctp_instruments"
	ctpInstrumentSyncLogTable = "ctp_instrument_sync_log"
	ctpCommissionRateTable    = "ctp_commission_rates"
	ctpMarginRateTable        = "ctp_margin_rates"
	ctpProductExchangeTable   = "ctp_product_exchange"
)

type instrumentSyncLog struct {
	TradingDay      string
	InstrumentCount int
	UpdatedAt       time.Time
}

type InstrumentCatalogRecord struct {
	InstrumentID           string
	ExchangeID             string
	ExchangeInstID         string
	InstrumentName         string
	ProductID              string
	ProductClass           byte
	DeliveryYear           int
	DeliveryMonth          int
	MaxMarketOrderVolume   int
	MinMarketOrderVolume   int
	MaxLimitOrderVolume    int
	MinLimitOrderVolume    int
	VolumeMultiple         int
	PriceTick              float64
	CreateDate             string
	OpenDate               string
	ExpireDate             string
	StartDelivDate         string
	EndDelivDate           string
	InstLifePhase          byte
	IsTrading              int
	PositionType           byte
	PositionDateType       byte
	LongMarginRatio        float64
	ShortMarginRatio       float64
	MaxMarginSideAlgorithm byte
	UnderlyingInstrID      string
	StrikePrice            float64
	OptionsType            byte
	UnderlyingMultiple     float64
	CombinationType        byte
	TradingDay             string
	SyncTradingDay         string
	UpdatedAt              time.Time
}

type InstrumentCatalogRepo struct {
	db *sql.DB
}

type ProductExchangeRecord struct {
	ProductID      string
	ProductIDNorm  string
	ExchangeID     string
	ProductClass   byte
	VolumeMultiple int
	PriceTick      float64
	UpdatedAt      time.Time
}

type productExchangeRecord struct {
	ProductID      string
	ProductIDNorm  string
	ExchangeID     string
	ProductClass   byte
	VolumeMultiple int
	PriceTick      float64
}

type CommissionRateRecord struct {
	InstrumentID            string
	ExchangeID              string
	OpenRatioByMoney        float64
	OpenRatioByVolume       float64
	CloseRatioByMoney       float64
	CloseRatioByVolume      float64
	CloseTodayRatioByMoney  float64
	CloseTodayRatioByVolume float64
	SyncTradingDay          string
	UpdatedAt               time.Time
}

type MarginRateRecord struct {
	InstrumentID             string
	ExchangeID               string
	HedgeFlag                string
	LongMarginRatioByMoney   float64
	LongMarginRatioByVolume  float64
	ShortMarginRatioByMoney  float64
	ShortMarginRatioByVolume float64
	IsRelative               int
	SyncTradingDay           string
	UpdatedAt                time.Time
}

func NewInstrumentCatalogRepo(db *sql.DB) *InstrumentCatalogRepo {
	if db == nil {
		return nil
	}
	return &InstrumentCatalogRepo{db: db}
}

func (r *InstrumentCatalogRepo) LatestSyncLog(tradingDay string) (instrumentSyncLog, bool, error) {
	if r == nil || r.db == nil || strings.TrimSpace(tradingDay) == "" {
		return instrumentSyncLog{}, false, nil
	}

	var out instrumentSyncLog
	err := r.db.QueryRow(
		`SELECT trading_day,instrument_count,updated_at
FROM ctp_instrument_sync_log
WHERE trading_day=?`,
		strings.TrimSpace(tradingDay),
	).Scan(&out.TradingDay, &out.InstrumentCount, &out.UpdatedAt)
	if err == sql.ErrNoRows {
		return instrumentSyncLog{}, false, nil
	}
	if err != nil {
		return instrumentSyncLog{}, false, fmt.Errorf("query instrument sync log failed: %w", err)
	}
	return out, true, nil
}

func (r *InstrumentCatalogRepo) ListInstrumentInfosByTradingDay(tradingDay string) ([]instrumentInfo, error) {
	records, err := r.ListByTradingDay(tradingDay)
	if err != nil {
		return nil, err
	}
	out := make([]instrumentInfo, 0, len(records))
	for _, item := range records {
		out = append(out, instrumentInfo{
			ID:           item.InstrumentID,
			ExchangeID:   item.ExchangeID,
			ProductID:    item.ProductID,
			ProductClass: item.ProductClass,
		})
	}
	return dedupeInstrumentInfos(out), nil
}

func (r *InstrumentCatalogRepo) ListByTradingDay(tradingDay string) ([]InstrumentCatalogRecord, error) {
	if r == nil || r.db == nil || strings.TrimSpace(tradingDay) == "" {
		return nil, nil
	}

	rows, err := r.db.Query(
		`SELECT instrument_id,exchange_id,exchange_inst_id,instrument_name,product_id,product_class,
delivery_year,delivery_month,max_market_order_volume,min_market_order_volume,
max_limit_order_volume,min_limit_order_volume,volume_multiple,price_tick,create_date,
open_date,expire_date,start_deliv_date,end_deliv_date,inst_life_phase,is_trading,
position_type,position_date_type,long_margin_ratio,short_margin_ratio,max_margin_side_algorithm,
underlying_instr_id,strike_price,options_type,underlying_multiple,combination_type,trading_day,sync_trading_day,updated_at
FROM ctp_instruments
WHERE trading_day=? OR sync_trading_day=?
ORDER BY exchange_id ASC, product_id ASC, instrument_id ASC`,
		strings.TrimSpace(tradingDay),
		strings.TrimSpace(tradingDay),
	)
	if err != nil {
		return nil, fmt.Errorf("query instrument catalog failed: %w", err)
	}
	defer rows.Close()

	var out []InstrumentCatalogRecord
	for rows.Next() {
		var item InstrumentCatalogRecord
		var productClass string
		var instLifePhase string
		var positionType string
		var positionDateType string
		var maxMarginSideAlgorithm string
		var optionsType string
		var combinationType string
		if err := rows.Scan(
			&item.InstrumentID,
			&item.ExchangeID,
			&item.ExchangeInstID,
			&item.InstrumentName,
			&item.ProductID,
			&productClass,
			&item.DeliveryYear,
			&item.DeliveryMonth,
			&item.MaxMarketOrderVolume,
			&item.MinMarketOrderVolume,
			&item.MaxLimitOrderVolume,
			&item.MinLimitOrderVolume,
			&item.VolumeMultiple,
			&item.PriceTick,
			&item.CreateDate,
			&item.OpenDate,
			&item.ExpireDate,
			&item.StartDelivDate,
			&item.EndDelivDate,
			&instLifePhase,
			&item.IsTrading,
			&positionType,
			&positionDateType,
			&item.LongMarginRatio,
			&item.ShortMarginRatio,
			&maxMarginSideAlgorithm,
			&item.UnderlyingInstrID,
			&item.StrikePrice,
			&optionsType,
			&item.UnderlyingMultiple,
			&combinationType,
			&item.TradingDay,
			&item.SyncTradingDay,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan instrument catalog failed: %w", err)
		}
		item.ProductClass = byteFromDBString(productClass)
		item.InstLifePhase = byteFromDBString(instLifePhase)
		item.PositionType = byteFromDBString(positionType)
		item.PositionDateType = byteFromDBString(positionDateType)
		item.MaxMarginSideAlgorithm = byteFromDBString(maxMarginSideAlgorithm)
		item.OptionsType = byteFromDBString(optionsType)
		item.CombinationType = byteFromDBString(combinationType)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate instrument catalog failed: %w", err)
	}
	return dedupeInstrumentCatalogRecords(out), nil
}

func (r *InstrumentCatalogRepo) Get(instrumentID string, exchangeID string) (InstrumentCatalogRecord, bool, error) {
	if r == nil || r.db == nil {
		return InstrumentCatalogRecord{}, false, nil
	}
	instrumentID = strings.TrimSpace(instrumentID)
	exchangeID = strings.TrimSpace(exchangeID)
	if instrumentID == "" || exchangeID == "" {
		return InstrumentCatalogRecord{}, false, nil
	}

	var item InstrumentCatalogRecord
	var productClass string
	var instLifePhase string
	var positionType string
	var positionDateType string
	var maxMarginSideAlgorithm string
	var optionsType string
	var combinationType string
	err := r.db.QueryRow(
		`SELECT instrument_id,exchange_id,exchange_inst_id,instrument_name,product_id,product_class,
delivery_year,delivery_month,max_market_order_volume,min_market_order_volume,
max_limit_order_volume,min_limit_order_volume,volume_multiple,price_tick,create_date,
open_date,expire_date,start_deliv_date,end_deliv_date,inst_life_phase,is_trading,
position_type,position_date_type,long_margin_ratio,short_margin_ratio,max_margin_side_algorithm,
underlying_instr_id,strike_price,options_type,underlying_multiple,combination_type,trading_day,sync_trading_day,updated_at
FROM ctp_instruments
WHERE instrument_id=? AND exchange_id=?`,
		instrumentID,
		exchangeID,
	).Scan(
		&item.InstrumentID,
		&item.ExchangeID,
		&item.ExchangeInstID,
		&item.InstrumentName,
		&item.ProductID,
		&productClass,
		&item.DeliveryYear,
		&item.DeliveryMonth,
		&item.MaxMarketOrderVolume,
		&item.MinMarketOrderVolume,
		&item.MaxLimitOrderVolume,
		&item.MinLimitOrderVolume,
		&item.VolumeMultiple,
		&item.PriceTick,
		&item.CreateDate,
		&item.OpenDate,
		&item.ExpireDate,
		&item.StartDelivDate,
		&item.EndDelivDate,
		&instLifePhase,
		&item.IsTrading,
		&positionType,
		&positionDateType,
		&item.LongMarginRatio,
		&item.ShortMarginRatio,
		&maxMarginSideAlgorithm,
		&item.UnderlyingInstrID,
		&item.StrikePrice,
		&optionsType,
		&item.UnderlyingMultiple,
		&combinationType,
		&item.TradingDay,
		&item.SyncTradingDay,
		&item.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return InstrumentCatalogRecord{}, false, nil
	}
	if err != nil {
		return InstrumentCatalogRecord{}, false, fmt.Errorf("query instrument catalog record failed: %w", err)
	}
	item.ProductClass = byteFromDBString(productClass)
	item.InstLifePhase = byteFromDBString(instLifePhase)
	item.PositionType = byteFromDBString(positionType)
	item.PositionDateType = byteFromDBString(positionDateType)
	item.MaxMarginSideAlgorithm = byteFromDBString(maxMarginSideAlgorithm)
	item.OptionsType = byteFromDBString(optionsType)
	item.CombinationType = byteFromDBString(combinationType)
	return item, true, nil
}

func (r *InstrumentCatalogRepo) SyncTradingDay(tradingDay string, instruments []instrumentSnapshot, syncedAt time.Time) (int, error) {
	if r == nil || r.db == nil || strings.TrimSpace(tradingDay) == "" || len(instruments) == 0 {
		return 0, nil
	}

	tx, err := r.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin instrument catalog sync failed: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(
		`REPLACE INTO ctp_instruments(
instrument_id,exchange_id,exchange_inst_id,instrument_name,product_id,product_class,
delivery_year,delivery_month,max_market_order_volume,min_market_order_volume,
max_limit_order_volume,min_limit_order_volume,volume_multiple,price_tick,create_date,
open_date,expire_date,start_deliv_date,end_deliv_date,inst_life_phase,is_trading,
position_type,position_date_type,long_margin_ratio,short_margin_ratio,max_margin_side_algorithm,
underlying_instr_id,strike_price,options_type,underlying_multiple,combination_type,
trading_day,sync_trading_day,updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
	)
	if err != nil {
		return 0, fmt.Errorf("prepare instrument catalog sync failed: %w", err)
	}
	defer stmt.Close()

	deduped := dedupeInstrumentSnapshots(instruments)
	productRecords := dedupeProductExchangeRecords(deduped)
	for _, item := range deduped {
		if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.ExchangeID) == "" {
			continue
		}
		if _, err = stmt.Exec(
			item.ID,
			item.ExchangeID,
			item.ExchangeInstID,
			item.InstrumentName,
			item.ProductID,
			byteToDBString(item.ProductClass),
			item.DeliveryYear,
			item.DeliveryMonth,
			item.MaxMarketOrderVolume,
			item.MinMarketOrderVolume,
			item.MaxLimitOrderVolume,
			item.MinLimitOrderVolume,
			item.VolumeMultiple,
			item.PriceTick,
			item.CreateDate,
			item.OpenDate,
			item.ExpireDate,
			item.StartDelivDate,
			item.EndDelivDate,
			byteToDBString(item.InstLifePhase),
			item.IsTrading,
			byteToDBString(item.PositionType),
			byteToDBString(item.PositionDateType),
			item.LongMarginRatio,
			item.ShortMarginRatio,
			byteToDBString(item.MaxMarginSideAlgorithm),
			item.UnderlyingInstrID,
			item.StrikePrice,
			byteToDBString(item.OptionsType),
			item.UnderlyingMultiple,
			byteToDBString(item.CombinationType),
			strings.TrimSpace(tradingDay),
			strings.TrimSpace(tradingDay),
			syncedAt,
		); err != nil {
			return 0, fmt.Errorf("upsert instrument catalog failed: %w", err)
		}
	}

	if err := replaceProductExchangeRecords(tx, productRecords, syncedAt); err != nil {
		return 0, err
	}

	if _, err = tx.Exec(
		`REPLACE INTO ctp_instrument_sync_log(trading_day,instrument_count,updated_at) VALUES (?,?,?)`,
		strings.TrimSpace(tradingDay),
		len(deduped),
		syncedAt,
	); err != nil {
		return 0, fmt.Errorf("upsert instrument sync log failed: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit instrument catalog sync failed: %w", err)
	}

	logger.Info(
		"instrument catalog synced",
		"trading_day", strings.TrimSpace(tradingDay),
		"instrument_count", len(deduped),
		"updated_product_count", len(productRecords),
		"updated_at", syncedAt.Format("2006-01-02 15:04:05"),
	)
	return len(productRecords), nil
}

func (r *InstrumentCatalogRepo) SyncCommissionRates(tradingDay string, items []commissionRateSnapshot, syncedAt time.Time) (int, error) {
	if r == nil || r.db == nil || strings.TrimSpace(tradingDay) == "" || len(items) == 0 {
		return 0, nil
	}
	tx, err := r.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin commission rate sync failed: %w", err)
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
		return 0, fmt.Errorf("prepare commission rate sync failed: %w", err)
	}
	defer stmt.Close()

	deduped := dedupeCommissionRateSnapshots(items)
	for _, item := range deduped {
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
			syncedAt,
		); err != nil {
			return 0, fmt.Errorf("upsert commission rate failed: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit commission rate sync failed: %w", err)
	}
	logger.Info(
		"commission rates synced",
		"trading_day", strings.TrimSpace(tradingDay),
		"row_count", len(deduped),
		"updated_at", syncedAt.Format("2006-01-02 15:04:05"),
	)
	return len(deduped), nil
}

func (r *InstrumentCatalogRepo) ListCommissionRatesByTradingDay(tradingDay string) ([]CommissionRateRecord, error) {
	if r == nil || r.db == nil || strings.TrimSpace(tradingDay) == "" {
		return nil, nil
	}
	rows, err := r.db.Query(
		`SELECT instrument_id,exchange_id,open_ratio_by_money,open_ratio_by_volume,
close_ratio_by_money,close_ratio_by_volume,close_today_ratio_by_money,close_today_ratio_by_volume,
sync_trading_day,updated_at
FROM ctp_commission_rates
WHERE sync_trading_day=?
ORDER BY exchange_id ASC, instrument_id ASC`,
		strings.TrimSpace(tradingDay),
	)
	if err != nil {
		return nil, fmt.Errorf("query commission rates failed: %w", err)
	}
	defer rows.Close()
	out := make([]CommissionRateRecord, 0, 256)
	for rows.Next() {
		var item CommissionRateRecord
		if err := rows.Scan(
			&item.InstrumentID,
			&item.ExchangeID,
			&item.OpenRatioByMoney,
			&item.OpenRatioByVolume,
			&item.CloseRatioByMoney,
			&item.CloseRatioByVolume,
			&item.CloseTodayRatioByMoney,
			&item.CloseTodayRatioByVolume,
			&item.SyncTradingDay,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan commission rates failed: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate commission rates failed: %w", err)
	}
	return out, nil
}

func (r *InstrumentCatalogRepo) ListMarginRatesByTradingDay(tradingDay string) ([]MarginRateRecord, error) {
	if r == nil || r.db == nil || strings.TrimSpace(tradingDay) == "" {
		return nil, nil
	}
	rows, err := r.db.Query(
		`SELECT instrument_id,exchange_id,hedge_flag,long_margin_ratio_by_money,long_margin_ratio_by_volume,
short_margin_ratio_by_money,short_margin_ratio_by_volume,is_relative,sync_trading_day,updated_at
FROM ctp_margin_rates
WHERE sync_trading_day=?
ORDER BY exchange_id ASC, instrument_id ASC, hedge_flag ASC`,
		strings.TrimSpace(tradingDay),
	)
	if err != nil {
		return nil, fmt.Errorf("query margin rates failed: %w", err)
	}
	defer rows.Close()
	out := make([]MarginRateRecord, 0, 256)
	for rows.Next() {
		var item MarginRateRecord
		if err := rows.Scan(
			&item.InstrumentID,
			&item.ExchangeID,
			&item.HedgeFlag,
			&item.LongMarginRatioByMoney,
			&item.LongMarginRatioByVolume,
			&item.ShortMarginRatioByMoney,
			&item.ShortMarginRatioByVolume,
			&item.IsRelative,
			&item.SyncTradingDay,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan margin rates failed: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate margin rates failed: %w", err)
	}
	return out, nil
}

func (r *InstrumentCatalogRepo) GetProductExchange(productID string, exchangeID string) (ProductExchangeRecord, bool, error) {
	if r == nil || r.db == nil {
		return ProductExchangeRecord{}, false, nil
	}
	productIDNorm := normalizeProductID(productID)
	exchangeID = strings.TrimSpace(exchangeID)
	if productIDNorm == "" || exchangeID == "" {
		return ProductExchangeRecord{}, false, nil
	}
	var item ProductExchangeRecord
	var productClass string
	err := r.db.QueryRow(
		`SELECT product_id,product_id_norm,exchange_id,product_class,volume_multiple,price_tick,updated_at
FROM ctp_product_exchange
WHERE product_id_norm=? AND exchange_id=?`,
		productIDNorm,
		exchangeID,
	).Scan(
		&item.ProductID,
		&item.ProductIDNorm,
		&item.ExchangeID,
		&productClass,
		&item.VolumeMultiple,
		&item.PriceTick,
		&item.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return ProductExchangeRecord{}, false, nil
	}
	if err != nil {
		return ProductExchangeRecord{}, false, fmt.Errorf("query product exchange record failed: %w", err)
	}
	item.ProductClass = byteFromDBString(productClass)
	return item, true, nil
}

func (r *InstrumentCatalogRepo) ListProductExchangesByProduct(productID string) ([]ProductExchangeRecord, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	productIDNorm := normalizeProductID(productID)
	if productIDNorm == "" {
		return nil, nil
	}
	rows, err := r.db.Query(
		`SELECT product_id,product_id_norm,exchange_id,product_class,volume_multiple,price_tick,updated_at
FROM ctp_product_exchange
WHERE product_id_norm=?
ORDER BY exchange_id ASC`,
		productIDNorm,
	)
	if err != nil {
		return nil, fmt.Errorf("query product exchange records failed: %w", err)
	}
	defer rows.Close()

	var out []ProductExchangeRecord
	for rows.Next() {
		var item ProductExchangeRecord
		var productClass string
		if err := rows.Scan(
			&item.ProductID,
			&item.ProductIDNorm,
			&item.ExchangeID,
			&productClass,
			&item.VolumeMultiple,
			&item.PriceTick,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan product exchange record failed: %w", err)
		}
		item.ProductClass = byteFromDBString(productClass)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate product exchange records failed: %w", err)
	}
	return out, nil
}

func (r *InstrumentCatalogRepo) ListAllProductExchanges() ([]ProductExchangeRecord, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	rows, err := r.db.Query(
		`SELECT product_id,product_id_norm,exchange_id,product_class,volume_multiple,price_tick,updated_at
FROM ctp_product_exchange
ORDER BY product_id_norm ASC, exchange_id ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("query all product exchange records failed: %w", err)
	}
	defer rows.Close()

	var out []ProductExchangeRecord
	for rows.Next() {
		var item ProductExchangeRecord
		var productClass string
		if err := rows.Scan(
			&item.ProductID,
			&item.ProductIDNorm,
			&item.ExchangeID,
			&productClass,
			&item.VolumeMultiple,
			&item.PriceTick,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan all product exchange records failed: %w", err)
		}
		item.ProductClass = byteFromDBString(productClass)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all product exchange records failed: %w", err)
	}
	return out, nil
}

func replaceProductExchangeRecords(tx *sql.Tx, items []productExchangeRecord, syncedAt time.Time) error {
	if tx == nil || len(items) == 0 {
		return nil
	}

	deleteSQL, deleteArgs := buildProductExchangeDeleteSQL(items)
	if deleteSQL != "" {
		if _, err := tx.Exec(deleteSQL, deleteArgs...); err != nil {
			return fmt.Errorf("delete product exchange rows failed: %w", err)
		}
	}

	insertSQL, insertArgs := buildProductExchangeInsertSQL(items, syncedAt)
	if insertSQL == "" {
		return nil
	}
	if _, err := tx.Exec(insertSQL, insertArgs...); err != nil {
		return fmt.Errorf("insert product exchange rows failed: %w", err)
	}
	return nil
}

func buildProductExchangeDeleteSQL(items []productExchangeRecord) (string, []any) {
	if len(items) == 0 {
		return "", nil
	}
	clauses := make([]string, 0, len(items))
	args := make([]any, 0, len(items)*2)
	for _, item := range items {
		if strings.TrimSpace(item.ProductIDNorm) == "" || strings.TrimSpace(item.ExchangeID) == "" {
			continue
		}
		clauses = append(clauses, "(product_id_norm=? AND exchange_id=?)")
		args = append(args, item.ProductIDNorm, item.ExchangeID)
	}
	if len(clauses) == 0 {
		return "", nil
	}
	return `DELETE FROM ctp_product_exchange WHERE ` + strings.Join(clauses, " OR "), args
}

func buildProductExchangeInsertSQL(items []productExchangeRecord, syncedAt time.Time) (string, []any) {
	if len(items) == 0 {
		return "", nil
	}
	values := make([]string, 0, len(items))
	args := make([]any, 0, len(items)*7)
	for _, item := range items {
		if strings.TrimSpace(item.ProductID) == "" || strings.TrimSpace(item.ProductIDNorm) == "" || strings.TrimSpace(item.ExchangeID) == "" {
			continue
		}
		values = append(values, "(?,?,?,?,?,?,?)")
		args = append(args,
			item.ProductID,
			item.ProductIDNorm,
			item.ExchangeID,
			byteToDBString(item.ProductClass),
			item.VolumeMultiple,
			item.PriceTick,
			syncedAt,
		)
	}
	if len(values) == 0 {
		return "", nil
	}
	return `INSERT INTO ctp_product_exchange(
product_id,product_id_norm,exchange_id,product_class,volume_multiple,price_tick,updated_at
) VALUES ` + strings.Join(values, ","), args
}

func dedupeInstrumentCatalogRecords(items []InstrumentCatalogRecord) []InstrumentCatalogRecord {
	if len(items) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(items))
	out := make([]InstrumentCatalogRecord, 0, len(items))
	for _, item := range items {
		if item.InstrumentID == "" {
			continue
		}
		key := strings.TrimSpace(item.ExchangeID) + "|" + strings.TrimSpace(item.InstrumentID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func dedupeInstrumentSnapshots(items []instrumentSnapshot) []instrumentSnapshot {
	if len(items) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(items))
	out := make([]instrumentSnapshot, 0, len(items))
	for _, item := range items {
		if item.ID == "" {
			continue
		}
		key := strings.TrimSpace(item.ExchangeID) + "|" + strings.TrimSpace(item.ID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func dedupeProductExchangeRecords(items []instrumentSnapshot) []productExchangeRecord {
	if len(items) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(items))
	out := make([]productExchangeRecord, 0, len(items))
	for _, item := range items {
		productID := strings.TrimSpace(item.ProductID)
		productIDNorm := normalizeProductID(productID)
		exchangeID := strings.TrimSpace(item.ExchangeID)
		if productID == "" || productIDNorm == "" || exchangeID == "" {
			continue
		}
		key := exchangeID + "|" + productIDNorm
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, productExchangeRecord{
			ProductID:      productID,
			ProductIDNorm:  productIDNorm,
			ExchangeID:     exchangeID,
			ProductClass:   item.ProductClass,
			VolumeMultiple: item.VolumeMultiple,
			PriceTick:      item.PriceTick,
		})
	}
	return out
}

func dedupeCommissionRateSnapshots(items []commissionRateSnapshot) []commissionRateSnapshot {
	if len(items) == 0 {
		return nil
	}
	latest := make(map[string]commissionRateSnapshot, len(items))
	for _, item := range items {
		key := strings.ToLower(strings.TrimSpace(item.InstrumentID)) + "|" + strings.TrimSpace(item.ExchangeID)
		if key == "|" {
			continue
		}
		latest[key] = item
	}
	out := make([]commissionRateSnapshot, 0, len(latest))
	for _, item := range latest {
		out = append(out, item)
	}
	return out
}

func normalizeProductID(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func byteToDBString(v byte) string {
	if v == 0 {
		return ""
	}
	return string([]byte{v})
}

func byteFromDBString(v string) byte {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	return v[0]
}
