package trader

import "database/sql"

// 以下导出用于对外复用与测试调用。

type MinuteBar = minuteBar
type KlineStore = klineStore
type MdSpi = mdSpi
type L9AsyncCalculator = l9AsyncCalculator
type InstrumentInfo = instrumentInfo

const (
	ColInstrumentID = colInstrumentID
	ColExchange     = colExchange
	ColDataTime     = colTime
	ColTime         = colTime
	ColAdjustedTime = colAdjustedTime
	ColPeriod       = colPeriod
	ColOpen         = colOpen
	ColHigh         = colHigh
	ColLow          = colLow
	ColClose        = colClose
	ColVolume       = colVolume
	ColOpenInterest = colOpenInterest
	ColSettlement   = colSettlement
)

func NewKlineStore(path string) (*klineStore, error) {
	return newKlineStore(path)
}

func (s *klineStore) DB() *sql.DB {
	return s.db
}

func NewMdSpi(store *klineStore, l9Async *l9AsyncCalculator) *mdSpi {
	return newMdSpi(store, l9Async)
}

func NewL9AsyncCalculator(store *klineStore, enabled bool, workers int, expectedByVariety map[string][]string) *l9AsyncCalculator {
	return newL9AsyncCalculator(store, enabled, workers, expectedByVariety)
}

func SelectSubscribeTargets(queriedInstruments []instrumentInfo, configuredVarieties []string) []string {
	return selectSubscribeTargets(queriedInstruments, configuredVarieties)
}

func NormalizeVariety(value string) string {
	return normalizeVariety(value)
}

func TableNameForVariety(variety string) (string, error) {
	return tableNameForVariety(variety)
}

func TableNameForL9Variety(variety string) (string, error) {
	return tableNameForL9Variety(variety)
}

func NormalizeInstrumentIDForTable(instrumentID string, tableName string) string {
	return normalizeInstrumentIDForTable(instrumentID, tableName)
}
