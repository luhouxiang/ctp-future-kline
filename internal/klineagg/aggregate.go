package klineagg

import (
	"sort"
	"time"

	"ctp-go-demo/internal/sessiontime"
)

type SessionRange struct {
	// Start 是交易时段起始分钟，按 HHMM 展开为分钟序号。
	Start int
	// End 是交易时段结束分钟，按 HHMM 展开为分钟序号。
	End int
}

type MinuteBar struct {
	// InstrumentID 是合约代码。
	InstrumentID string
	// Exchange 是交易所代码。
	Exchange string
	// DataTime 是该分钟线的业务时间。
	DataTime time.Time
	// AdjustedTime 是跨夜修正后的时间轴。
	AdjustedTime time.Time
	// Open 是开盘价。
	Open float64
	// High 是最高价。
	High float64
	// Low 是最低价。
	Low float64
	// Close 是收盘价。
	Close float64
	// Volume 是该分钟成交量。
	Volume int64
	// OpenInterest 是该分钟结束时持仓量。
	OpenInterest float64
}

type BucketStat struct {
	// TradingDay 是该统计所属交易日。
	TradingDay string
	// BucketKey 是聚合桶唯一键。
	BucketKey string
	// ExpectedMinutes 是该桶理论应包含的分钟数。
	ExpectedMinutes int
	// ActualMinutes 是该桶实际聚合到的分钟数。
	ActualMinutes int
}

type AggBar struct {
	// InstrumentID 是合约代码。
	InstrumentID string
	// Exchange 是交易所代码。
	Exchange string
	// DataTime 是聚合后 bar 的业务时间标签。
	DataTime time.Time
	// AdjustedTime 是聚合后 bar 的跨夜修正时间标签。
	AdjustedTime time.Time
	// Period 是目标周期名称，例如 5m、15m。
	Period string
	// Open 是聚合后开盘价。
	Open float64
	// High 是聚合后最高价。
	High float64
	// Low 是聚合后最低价。
	Low float64
	// Close 是聚合后收盘价。
	Close float64
	// Volume 是聚合后总成交量。
	Volume int64
	// OpenInterest 是聚合结束时持仓量。
	OpenInterest float64
	// ExpectedMinutes 是该桶理论应包含的分钟数。
	ExpectedMinutes int
	// ActualMinutes 是该桶实际聚合到的分钟数。
	ActualMinutes int
}

type Options struct {
	// CrossSessionFor30m1h 控制 30m/60m 是否允许跨交易时段拼接。
	CrossSessionFor30m1h bool
	// ClampToSessionEnd 控制是否将标签时间钳制到时段结束点。
	ClampToSessionEnd bool
	// ComputeBucketStats 控制是否返回每个聚合桶的完整度统计。
	ComputeBucketStats bool
}

type minuteMeta struct {
	// globalSeq 是该分钟在整日交易分钟序列中的位置。
	globalSeq int
	// sessionID 是该分钟所属交易时段编号。
	sessionID int
	// sessionSeq 是该分钟在时段内部的顺序编号。
	sessionSeq int
}

type bucketKey struct {
	// day 是交易日字符串。
	day string
	// session 是交易时段编号。
	session int
	// bucketNo 是该时段内的桶编号。
	bucketNo int
}

func Aggregate(bars []MinuteBar, sessions []SessionRange, period string, minutes int, opts Options) ([]AggBar, []BucketStat) {
	if len(bars) == 0 || minutes <= 1 || len(sessions) == 0 {
		return nil, nil
	}
	sort.Slice(sessions, func(i, j int) bool {
		return tradingMinuteOrderKey(sessions[i].Start) < tradingMinuteOrderKey(sessions[j].Start)
	})

	minuteOrder, minuteMap, sessionMinutes := buildSessionMinuteMaps(sessions)
	if len(minuteOrder) == 0 || len(minuteMap) == 0 {
		return nil, nil
	}

	cross := opts.CrossSessionFor30m1h && (minutes == 30 || minutes == 60)

	out := make([]AggBar, 0, len(bars)/minutes+8)
	states := make(map[bucketKey]int, len(out))
	bucketExpected := make(map[bucketKey]int)
	bucketActual := make(map[bucketKey]int)

	for _, b := range bars {
		if b.DataTime.IsZero() {
			continue
		}
		m := b.DataTime.Hour()*60 + b.DataTime.Minute()
		meta, ok := minuteMap[m]
		if !ok {
			continue
		}
		day := b.DataTime.Format("2006-01-02")
		bk := buildBucketKey(day, meta, minutes, cross)

		expected := expectedMinutesForBucket(minutes, bk, cross, minuteOrder, sessionMinutes)
		if expected <= 0 {
			continue
		}
		bucketExpected[bk] = expected
		bucketActual[bk] += 1

		labelMinute := bucketLabelMinute(minutes, bk, cross, sessions, minuteOrder)
		if labelMinute < 0 {
			continue
		}
		labelDT := labelTime(b.DataTime, labelMinute)
		labelAdjusted := labelTime(b.AdjustedTime, labelMinute)
		if !cross && opts.ClampToSessionEnd {
			if sessionEnd := sessionEndForMinute(m, sessions); sessionEnd >= 0 {
				endDT := labelTime(b.DataTime, sessionEnd)
				endAdjusted := labelTime(b.AdjustedTime, sessionEnd)
				if labelDT.After(endDT) {
					labelDT = endDT
					labelAdjusted = endAdjusted
				}
			}
		}

		idx, exists := states[bk]
		if !exists {
			states[bk] = len(out)
			out = append(out, AggBar{
				InstrumentID:    b.InstrumentID,
				Exchange:        b.Exchange,
				DataTime:        labelDT,
				AdjustedTime:    labelAdjusted,
				Period:          period,
				Open:            b.Open,
				High:            b.High,
				Low:             b.Low,
				Close:           b.Close,
				Volume:          b.Volume,
				OpenInterest:    b.OpenInterest,
				ExpectedMinutes: expected,
				ActualMinutes:   1,
			})
			continue
		}

		a := out[idx]
		if b.High > a.High {
			a.High = b.High
		}
		if b.Low < a.Low {
			a.Low = b.Low
		}
		a.Close = b.Close
		a.Volume += b.Volume
		a.OpenInterest = b.OpenInterest
		a.DataTime = labelDT
		a.AdjustedTime = labelAdjusted
		a.ExpectedMinutes = expected
		a.ActualMinutes = bucketActual[bk]
		out[idx] = a
	}

	stats := make([]BucketStat, 0, len(bucketExpected))
	if opts.ComputeBucketStats {
		for k, exp := range bucketExpected {
			stats = append(stats, BucketStat{TradingDay: k.day, BucketKey: renderBucketKey(k), ExpectedMinutes: exp, ActualMinutes: bucketActual[k]})
		}
		sort.Slice(stats, func(i, j int) bool {
			if stats[i].TradingDay == stats[j].TradingDay {
				return stats[i].BucketKey < stats[j].BucketKey
			}
			return stats[i].TradingDay < stats[j].TradingDay
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].AdjustedTime.Equal(out[j].AdjustedTime) {
			return out[i].InstrumentID < out[j].InstrumentID
		}
		return out[i].AdjustedTime.Before(out[j].AdjustedTime)
	})
	return out, stats
}

func buildSessionMinuteMaps(sessions []SessionRange) ([]int, map[int]minuteMeta, map[int]int) {
	ranges := make([]sessiontime.Range, 0, len(sessions))
	for _, s := range sessions {
		ranges = append(ranges, sessiontime.Range{Start: s.Start, End: s.End})
	}
	labelOrder, labelMap, sessionMinutes := sessiontime.BuildLabelMinuteMaps(ranges)
	minuteOrder := make([]int, 0, len(labelOrder))
	minuteMap := make(map[int]minuteMeta, len(labelMap))
	for _, m := range labelOrder {
		meta := labelMap[m]
		minuteOrder = append(minuteOrder, m)
		minuteMap[m] = minuteMeta{
			globalSeq:  meta.GlobalSeq,
			sessionID:  meta.SessionID,
			sessionSeq: meta.SessionSeq,
		}
	}
	return minuteOrder, minuteMap, sessionMinutes
}

func buildBucketKey(day string, meta minuteMeta, minutes int, cross bool) bucketKey {
	if cross {
		return bucketKey{day: day, session: -1, bucketNo: meta.globalSeq / minutes}
	}
	return bucketKey{day: day, session: meta.sessionID, bucketNo: meta.sessionSeq / minutes}
}

func expectedMinutesForBucket(minutes int, key bucketKey, cross bool, minuteOrder []int, sessionMinutes map[int]int) int {
	if cross {
		start := key.bucketNo * minutes
		if start >= len(minuteOrder) {
			return 0
		}
		left := len(minuteOrder) - start
		if left > minutes {
			return minutes
		}
		return left
	}
	total := sessionMinutes[key.session]
	start := key.bucketNo * minutes
	if total <= 0 || start >= total {
		return 0
	}
	left := total - start
	if left > minutes {
		return minutes
	}
	return left
}

func bucketLabelMinute(minutes int, key bucketKey, cross bool, sessions []SessionRange, minuteOrder []int) int {
	if cross {
		endExclusive := (key.bucketNo + 1) * minutes
		if endExclusive > 0 && endExclusive <= len(minuteOrder) {
			return minuteOrder[endExclusive-1]
		}
		if len(minuteOrder) == 0 {
			return -1
		}
		return minuteOrder[len(minuteOrder)-1]
	}
	if key.session < 0 || key.session >= len(sessions) {
		return -1
	}
	s := sessions[key.session]
	candidate := sessiontime.SessionLabelStart(sessiontime.Range{Start: s.Start, End: s.End}) + (key.bucketNo+1)*minutes - 1
	if candidate > s.End {
		candidate = s.End
	}
	return candidate
}

func sessionEndForMinute(m int, sessions []SessionRange) int {
	for _, s := range sessions {
		if m >= s.Start && m <= s.End {
			return s.End
		}
	}
	return -1
}

func labelTime(base time.Time, minuteOfDay int) time.Time {
	h := (minuteOfDay / 60) % 24
	m := minuteOfDay % 60
	return time.Date(base.Year(), base.Month(), base.Day(), h, m, 0, 0, time.Local)
}

func renderBucketKey(k bucketKey) string {
	return k.day + "|" + itoa(k.session) + "|" + itoa(k.bucketNo)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func tradingMinuteOrderKey(minute int) int {
	if minute >= 18*60 {
		return minute
	}
	return minute + 24*60
}
