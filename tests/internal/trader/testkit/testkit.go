package testkit

import "ctp-go-demo/internal/trader"

// tests/internal/trader 下共用的测试辅助出口。

type MinuteBar = trader.MinuteBar
type KlineStore = trader.KlineStore
type MdSpi = trader.MdSpi
type L9AsyncCalculator = trader.L9AsyncCalculator
type InstrumentInfo = trader.InstrumentInfo

const (
	ColInstrumentID = trader.ColInstrumentID
	ColExchange     = trader.ColExchange
	ColTime         = trader.ColTime
	ColAdjustedTime = trader.ColAdjustedTime
	ColPeriod       = trader.ColPeriod
	ColOpen         = trader.ColOpen
	ColHigh         = trader.ColHigh
	ColLow          = trader.ColLow
	ColClose        = trader.ColClose
	ColVolume       = trader.ColVolume
	ColOpenInterest = trader.ColOpenInterest
	ColSettlement   = trader.ColSettlement
)

func NewKlineStore(path string) (*trader.KlineStore, error) {
	return trader.NewKlineStore(path)
}

func NewMdSpi(store *trader.KlineStore, l9Async *trader.L9AsyncCalculator) *trader.MdSpi {
	return trader.NewMdSpi(store, l9Async)
}

func NewL9AsyncCalculator(store *trader.KlineStore, enabled bool, workers int, expectedByVariety map[string][]string) *trader.L9AsyncCalculator {
	return trader.NewL9AsyncCalculator(store, enabled, workers, expectedByVariety)
}

func SelectSubscribeTargets(queriedInstruments []trader.InstrumentInfo, configuredVarieties []string) []string {
	return trader.SelectSubscribeTargets(queriedInstruments, configuredVarieties)
}

func NormalizeVariety(value string) string {
	return trader.NormalizeVariety(value)
}

func TableNameForVariety(variety string) (string, error) {
	return trader.TableNameForVariety(variety)
}

func TableNameForL9Variety(variety string) (string, error) {
	return trader.TableNameForL9Variety(variety)
}
