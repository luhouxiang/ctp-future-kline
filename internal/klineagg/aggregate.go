package klineagg

import (
	"sort"
	"time"
)

type SessionRange struct {
	Start int
	End   int
}

type MinuteBar struct {
	InstrumentID string
	Exchange     string
	DataTime     time.Time
	AdjustedTime time.Time
	Open         float64
	High         float64
	Low          float64
	Close        float64
	Volume       int64
	OpenInterest float64
}

type BucketStat struct {
	TradingDay      string
	BucketKey       string
	ExpectedMinutes int
	ActualMinutes   int
}

type AggBar struct {
	InstrumentID    string
	Exchange        string
	DataTime        time.Time
	AdjustedTime    time.Time
	Period          string
	Open            float64
	High            float64
	Low             float64
	Close           float64
	Volume          int64
	OpenInterest    float64
	ExpectedMinutes int
	ActualMinutes   int
}

type Options struct {
	CrossSessionFor30m1h bool
	ClampToSessionEnd    bool
	ComputeBucketStats   bool
}

type minuteMeta struct {
	globalSeq  int
	sessionID  int
	sessionSeq int
}

type bucketKey struct {
	day      string
	session  int
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
		if !cross && opts.ClampToSessionEnd {
			if sessionEnd := sessionEndForMinute(m, sessions); sessionEnd >= 0 {
				endDT := labelTime(b.DataTime, sessionEnd)
				if labelDT.After(endDT) {
					labelDT = endDT
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
				AdjustedTime:    labelDT,
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
		a.AdjustedTime = labelDT
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
	minuteOrder := make([]int, 0, 1024)
	minuteMap := make(map[int]minuteMeta, 1024)
	sessionMinutes := make(map[int]int, len(sessions))
	global := 0
	for sid, s := range sessions {
		if s.End < s.Start {
			continue
		}
		local := 0
		for m := s.Start; m <= s.End; m += 1 {
			if _, exists := minuteMap[m]; exists {
				continue
			}
			minuteMap[m] = minuteMeta{globalSeq: global, sessionID: sid, sessionSeq: local}
			minuteOrder = append(minuteOrder, m)
			global += 1
			local += 1
		}
		sessionMinutes[sid] = local
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
		if endExclusive < len(minuteOrder) {
			return minuteOrder[endExclusive]
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
	candidate := s.Start + (key.bucketNo+1)*minutes
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
