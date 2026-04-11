package quotes

import "fmt"

func formatTickCSVLine(ev tickEvent, actionDay string) string {
	return fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d\n",
		ev.ReceivedAt.Format("2006-01-02 15:04:05.000"),
		ev.InstrumentID,
		ev.ExchangeID,
		ev.ExchangeInstID,
		ev.TradingDay,
		actionDay,
		ev.UpdateTime,
		ev.LastPrice,
		ev.PreSettlementPrice,
		ev.PreClosePrice,
		ev.PreOpenInterest,
		ev.OpenPrice,
		ev.HighestPrice,
		ev.LowestPrice,
		ev.Volume,
		ev.Turnover,
		ev.OpenInterest,
		ev.ClosePrice,
		ev.SettlementPrice,
		ev.UpperLimitPrice,
		ev.LowerLimitPrice,
		ev.AveragePrice,
		ev.PreDelta,
		ev.CurrDelta,
		ev.BidPrice1,
		ev.AskPrice1,
		ev.UpdateMillisec,
		ev.BidVolume1,
		ev.AskVolume1,
	)
}
