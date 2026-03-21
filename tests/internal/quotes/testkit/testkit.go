package testkit

import "ctp-future-kline/internal/quotes"

// tests/internal/quotes 下共用的测试辅助出口。
type MinuteBar = quotes.MinuteBar
type KlineStore = quotes.KlineStore
type MdSpi = quotes.MdSpi
type L9AsyncCalculator = quotes.L9AsyncCalculator
type InstrumentInfo = quotes.InstrumentInfo
type TickEvent = quotes.TickEvent

const (
	ColInstrumentID = quotes.ColInstrumentID
	ColExchange     = quotes.ColExchange
	ColTime         = quotes.ColTime
	ColAdjustedTime = quotes.ColAdjustedTime
	ColPeriod       = quotes.ColPeriod
	ColOpen         = quotes.ColOpen
	ColHigh         = quotes.ColHigh
	ColLow          = quotes.ColLow
	ColClose        = quotes.ColClose
	ColVolume       = quotes.ColVolume
	ColOpenInterest = quotes.ColOpenInterest
	ColSettlement   = quotes.ColSettlement
)

func NewKlineStore(path string) (*quotes.KlineStore, error) {
	return quotes.NewKlineStore(path)
}

func NewMdSpi(store *quotes.KlineStore, l9Async *quotes.L9AsyncCalculator) *quotes.MdSpi {
	return quotes.NewMdSpi(store, l9Async)
}

func NewL9AsyncCalculator(store *quotes.KlineStore, enabled bool, workers int, expectedByVariety map[string][]string) *quotes.L9AsyncCalculator {
	return quotes.NewL9AsyncCalculator(store, enabled, workers, expectedByVariety)
}

func SelectSubscribeTargets(queriedInstruments []quotes.InstrumentInfo, configuredVarieties []string) []string {
	return quotes.SelectSubscribeTargets(queriedInstruments, configuredVarieties)
}

func NormalizeVariety(value string) string {
	return quotes.NormalizeVariety(value)
}

func TableNameForVariety(variety string) (string, error) {
	return quotes.TableNameForVariety(variety)
}

func TableNameForL9Variety(variety string) (string, error) {
	return quotes.TableNameForL9Variety(variety)
}

func TableNameForInstrumentMMVariety(variety string) (string, error) {
	return quotes.TableNameForInstrumentMMVariety(variety)
}

func TableNameForL9MMVariety(variety string) (string, error) {
	return quotes.TableNameForL9MMVariety(variety)
}
