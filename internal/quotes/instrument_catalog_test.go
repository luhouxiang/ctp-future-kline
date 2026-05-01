package quotes

import (
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestInstrumentCatalogRepoSyncAndReload(t *testing.T) {
	t.Parallel()

	db := openInstrumentCatalogTestDB(t)
	repo := NewInstrumentCatalogRepo(db)
	now := time.Date(2026, 4, 7, 9, 30, 0, 0, time.UTC)
	snapshots := []instrumentSnapshot{
		{
			ID:                     "rb2605",
			ExchangeID:             "SHFE",
			ExchangeInstID:         "rb2605",
			InstrumentName:         "螺纹钢2605",
			ProductID:              "rb",
			ProductClass:           byte('1'),
			VolumeMultiple:         10,
			PriceTick:              1,
			IsTrading:              1,
			DeliveryYear:           2026,
			DeliveryMonth:          5,
			MaxLimitOrderVolume:    500,
			MinLimitOrderVolume:    1,
			MaxMarketOrderVolume:   200,
			MinMarketOrderVolume:   1,
			CreateDate:             "20250401",
			OpenDate:               "20250415",
			ExpireDate:             "20260515",
			StartDelivDate:         "20260516",
			EndDelivDate:           "20260520",
			LongMarginRatio:        0.12,
			ShortMarginRatio:       0.12,
			UnderlyingInstrID:      "",
			UnderlyingMultiple:     0,
			InstLifePhase:          byte('1'),
			PositionType:           byte('2'),
			PositionDateType:       byte('1'),
			MaxMarginSideAlgorithm: byte('1'),
			OptionsType:            0,
			CombinationType:        0,
		},
		{
			ID:           "SR605",
			ExchangeID:   "CZCE",
			ProductID:    "SR",
			ProductClass: byte('1'),
			PriceTick:    1,
			IsTrading:    1,
		},
	}

	updatedProductCount, err := repo.SyncTradingDay("20260407", snapshots, now)
	if err != nil {
		t.Fatalf("SyncTradingDay() error = %v", err)
	}
	if updatedProductCount != 2 {
		t.Fatalf("updatedProductCount = %d, want 2", updatedProductCount)
	}

	log, ok, err := repo.LatestSyncLog("20260407")
	if err != nil {
		t.Fatalf("LatestSyncLog() error = %v", err)
	}
	if !ok {
		t.Fatalf("LatestSyncLog() ok = false, want true")
	}
	if log.InstrumentCount != 2 {
		t.Fatalf("InstrumentCount = %d, want 2", log.InstrumentCount)
	}

	infos, err := repo.ListInstrumentInfosByTradingDay("20260407")
	if err != nil {
		t.Fatalf("ListInstrumentInfosByTradingDay() error = %v", err)
	}
	if len(infos) != 2 {
		t.Fatalf("len(ListInstrumentInfosByTradingDay()) = %d, want 2", len(infos))
	}
	if infos[0].ID != "SR605" || infos[1].ID != "rb2605" {
		t.Fatalf("loaded IDs = %#v, want [SR605 rb2605]", []string{infos[0].ID, infos[1].ID})
	}

	record, ok, err := repo.Get("rb2605", "SHFE")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("Get() ok = false, want true")
	}
	if record.InstrumentName != "螺纹钢2605" {
		t.Fatalf("InstrumentName = %q, want %q", record.InstrumentName, "螺纹钢2605")
	}

	var productRows int
	if err := db.QueryRow(`SELECT COUNT(1) FROM ctp_product_exchange`).Scan(&productRows); err != nil {
		t.Fatalf("count product rows error = %v", err)
	}
	if productRows != 2 {
		t.Fatalf("product row count = %d, want 2", productRows)
	}

	var volumeMultiple int
	var priceTick float64
	var productID string
	var productIDNorm string
	if err := db.QueryRow(`SELECT product_id,product_id_norm,volume_multiple,price_tick FROM ctp_product_exchange WHERE product_id_norm=? AND exchange_id=?`, "rb", "SHFE").Scan(&productID, &productIDNorm, &volumeMultiple, &priceTick); err != nil {
		t.Fatalf("load product exchange row error = %v", err)
	}
	if productID != "rb" || productIDNorm != "rb" {
		t.Fatalf("product exchange ids = (%q,%q), want (rb,rb)", productID, productIDNorm)
	}
	if volumeMultiple != 10 || priceTick != 1 {
		t.Fatalf("product exchange row = (%d,%v), want (10,1)", volumeMultiple, priceTick)
	}

	item, ok, err := repo.GetProductExchange("RB", "SHFE")
	if err != nil {
		t.Fatalf("GetProductExchange() error = %v", err)
	}
	if !ok {
		t.Fatal("GetProductExchange() ok = false, want true")
	}
	if item.ProductID != "rb" || item.ProductIDNorm != "rb" || item.ExchangeID != "SHFE" {
		t.Fatalf("unexpected product exchange record: %+v", item)
	}

	czceItem, ok, err := repo.GetProductExchange("sr", "CZCE")
	if err != nil {
		t.Fatalf("GetProductExchange() CZCE error = %v", err)
	}
	if !ok {
		t.Fatal("GetProductExchange() CZCE ok = false, want true")
	}
	if czceItem.ProductID != "SR" || czceItem.ProductIDNorm != "sr" || czceItem.ExchangeID != "CZCE" {
		t.Fatalf("unexpected CZCE product exchange record: %+v", czceItem)
	}
}

func TestInstrumentCatalogRepoSyncDedupByExchangeAndInstrument(t *testing.T) {
	t.Parallel()

	db := openInstrumentCatalogTestDB(t)
	repo := NewInstrumentCatalogRepo(db)
	now := time.Date(2026, 4, 7, 9, 35, 0, 0, time.UTC)
	snapshots := []instrumentSnapshot{
		{ID: "rb2605", ExchangeID: "SHFE", ProductID: "rb", ProductClass: byte('1')},
		{ID: "rb2605", ExchangeID: "SHFE", ProductID: "rb", ProductClass: byte('1'), PriceTick: 1},
	}

	updatedProductCount, err := repo.SyncTradingDay("20260407", snapshots, now)
	if err != nil {
		t.Fatalf("SyncTradingDay() error = %v", err)
	}
	if updatedProductCount != 1 {
		t.Fatalf("updatedProductCount = %d, want 1", updatedProductCount)
	}

	var rows int
	if err := db.QueryRow(`SELECT COUNT(1) FROM ctp_instruments WHERE sync_trading_day=?`, "20260407").Scan(&rows); err != nil {
		t.Fatalf("count synced rows error = %v", err)
	}
	if rows != 1 {
		t.Fatalf("synced row count = %d, want 1", rows)
	}

	var productRows int
	if err := db.QueryRow(`SELECT COUNT(1) FROM ctp_product_exchange WHERE product_id=? AND exchange_id=?`, "rb", "SHFE").Scan(&productRows); err != nil {
		t.Fatalf("count product rows error = %v", err)
	}
	if productRows != 1 {
		t.Fatalf("product row count = %d, want 1", productRows)
	}
}

func openInstrumentCatalogTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	stmts := []string{
		`CREATE TABLE ctp_instruments (
  instrument_id TEXT NOT NULL,
  exchange_id TEXT NOT NULL,
  exchange_inst_id TEXT NOT NULL,
  instrument_name TEXT NOT NULL,
  product_id TEXT NOT NULL,
  product_class TEXT NOT NULL,
  delivery_year INTEGER NOT NULL DEFAULT 0,
  delivery_month INTEGER NOT NULL DEFAULT 0,
  max_market_order_volume INTEGER NOT NULL DEFAULT 0,
  min_market_order_volume INTEGER NOT NULL DEFAULT 0,
  max_limit_order_volume INTEGER NOT NULL DEFAULT 0,
  min_limit_order_volume INTEGER NOT NULL DEFAULT 0,
  volume_multiple INTEGER NOT NULL DEFAULT 0,
  price_tick REAL NOT NULL DEFAULT 0,
  create_date TEXT NOT NULL,
  open_date TEXT NOT NULL,
  expire_date TEXT NOT NULL,
  start_deliv_date TEXT NOT NULL,
  end_deliv_date TEXT NOT NULL,
  inst_life_phase TEXT NOT NULL,
  is_trading INTEGER NOT NULL DEFAULT 0,
  position_type TEXT NOT NULL,
  position_date_type TEXT NOT NULL,
  long_margin_ratio REAL NOT NULL DEFAULT 0,
  short_margin_ratio REAL NOT NULL DEFAULT 0,
  max_margin_side_algorithm TEXT NOT NULL,
  underlying_instr_id TEXT NOT NULL,
  strike_price REAL NOT NULL DEFAULT 0,
  options_type TEXT NOT NULL,
  underlying_multiple REAL NOT NULL DEFAULT 0,
  combination_type TEXT NOT NULL,
  trading_day TEXT NOT NULL DEFAULT '',
  sync_trading_day TEXT NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (sync_trading_day, instrument_id, exchange_id)
)`,
		`CREATE TABLE ctp_instrument_sync_log (
  trading_day TEXT PRIMARY KEY,
  instrument_count INTEGER NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL
)`,
		`CREATE TABLE ctp_product_exchange (
  product_id TEXT NOT NULL,
  product_id_norm TEXT NOT NULL,
  exchange_id TEXT NOT NULL,
  product_class TEXT NOT NULL,
  volume_multiple INTEGER NOT NULL DEFAULT 0,
  price_tick REAL NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (product_id, exchange_id)
)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create test table error = %v", err)
		}
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}
