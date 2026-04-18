package klinecompose

import (
	"sort"
	"strings"
	"time"

	"ctp-future-kline/internal/sessiontime"
)

type Bar struct {
	AdjustedTime int64   `json:"adjusted_time"`
	DataTime     int64   `json:"data_time"`
	Open         float64 `json:"open"`
	High         float64 `json:"high"`
	Low          float64 `json:"low"`
	Close        float64 `json:"close"`
	Volume       int64   `json:"volume"`
	OpenInterest float64 `json:"open_interest"`
}

type Tick struct {
	UpdateTime     string  `json:"update_time"`
	UpdateMillisec int     `json:"update_millisec"`
	LastPrice      float64 `json:"last_price"`
	Volume         int64   `json:"volume"`
	OpenInterest   float64 `json:"open_interest"`
}

func ResolveLabelMinute(rawMinute int, sessions []sessiontime.Range) int {
	if rawMinute < 0 || rawMinute >= 24*60 {
		return 0
	}
	for _, s := range sortSessions(sessions) {
		startEdge := s.Start
		if s.Start > 0 {
			startEdge = s.Start - 1
		}
		if rawMinute < startEdge || rawMinute > s.End {
			continue
		}
		labelStart := s.End
		if s.Start < s.End {
			labelStart = s.Start + 1
		}
		label := rawMinute + 1
		if label < labelStart {
			label = labelStart
		}
		if label > s.End {
			label = s.End
		}
		return label
	}
	label := rawMinute + 1
	if label >= 24*60 {
		return label % (24 * 60)
	}
	return label
}

func TickTo1M(baseDay time.Time, tick Tick, sessions []sessiontime.Range, prev *Bar) (Bar, bool) {
	clockTime, err := time.ParseInLocation("15:04:05", strings.TrimSpace(tick.UpdateTime), time.Local)
	if err != nil {
		return Bar{}, false
	}
	rawMinute := clockTime.Hour()*60 + clockTime.Minute()
	labelMinute := ResolveLabelMinute(rawMinute, sessions)
	barTime := time.Date(baseDay.Year(), baseDay.Month(), baseDay.Day(), labelMinute/60, labelMinute%60, 0, 0, time.Local)
	bar := Bar{
		AdjustedTime: barTime.Unix(),
		DataTime:     barTime.Unix(),
		Open:         tick.LastPrice,
		High:         tick.LastPrice,
		Low:          tick.LastPrice,
		Close:        tick.LastPrice,
		Volume:       maxInt64(0, tick.Volume),
		OpenInterest: tick.OpenInterest,
	}
	if prev != nil && prev.AdjustedTime == bar.AdjustedTime {
		bar.Open = prev.Open
		bar.High = maxFloat(prev.High, tick.LastPrice)
		bar.Low = minFloat(prev.Low, tick.LastPrice)
	}
	return bar, true
}

func AggregateFrom1M(input []Bar, timeframe string) []Bar {
	minutes := timeframeMinutes(timeframe)
	if minutes <= 1 {
		return append([]Bar(nil), input...)
	}
	if len(input) == 0 {
		return nil
	}
	out := make([]Bar, 0, len(input)/minutes+2)
	var current *Bar
	var currentBucket int64
	for _, bar := range input {
		if bar.AdjustedTime <= 0 {
			continue
		}
		bucket := (bar.AdjustedTime / int64(minutes*60)) * int64(minutes*60)
		if current == nil || bucket != currentBucket {
			if current != nil {
				out = append(out, *current)
			}
			next := bar
			next.AdjustedTime = bucket
			next.DataTime = bucket
			current = &next
			currentBucket = bucket
			continue
		}
		current.High = maxFloat(current.High, bar.High)
		current.Low = minFloat(current.Low, bar.Low)
		current.Close = bar.Close
		current.Volume += maxInt64(0, bar.Volume)
		current.OpenInterest = bar.OpenInterest
	}
	if current != nil {
		out = append(out, *current)
	}
	return out
}

func timeframeMinutes(tf string) int {
	switch strings.ToLower(strings.TrimSpace(tf)) {
	case "1m":
		return 1
	case "5m":
		return 5
	case "15m":
		return 15
	case "30m":
		return 30
	case "1h":
		return 60
	case "1d":
		return 24 * 60
	default:
		return 1
	}
}

func sortSessions(in []sessiontime.Range) []sessiontime.Range {
	if len(in) == 0 {
		return sessiontime.DefaultRanges()
	}
	out := append([]sessiontime.Range(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		return tradingOrderKey(out[i].Start) < tradingOrderKey(out[j].Start)
	})
	return out
}

func tradingOrderKey(minute int) int {
	if minute >= 18*60 {
		return minute
	}
	return minute + 24*60
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
