package klineagg

import (
	"sort"
	"time"
)

type BucketPlan struct {
	Key             string
	TradingDay      string
	Period          string
	DataTime        time.Time
	AdjustedTime    time.Time
	ExpectedMinutes int
	CrossSession    bool
}

func PlanBucket(bar MinuteBar, sessions []SessionRange, period string, minutes int) (BucketPlan, bool) {
	if bar.DataTime.IsZero() || len(sessions) == 0 {
		return BucketPlan{}, false
	}
	if period == "1d" || minutes >= 1440 {
		return planDailyBucket(bar, sessions)
	}
	if minutes <= 1 {
		return BucketPlan{}, false
	}
	sortedSessions := append([]SessionRange(nil), sessions...)
	sort.Slice(sortedSessions, func(i, j int) bool {
		return tradingMinuteOrderKey(sortedSessions[i].Start) < tradingMinuteOrderKey(sortedSessions[j].Start)
	})
	minuteOrder, minuteMap, sessionMinutes := buildSessionMinuteMaps(sortedSessions)
	if len(minuteOrder) == 0 || len(minuteMap) == 0 {
		return BucketPlan{}, false
	}
	minuteOfDay := bar.DataTime.Hour()*60 + bar.DataTime.Minute()
	meta, ok := minuteMap[minuteOfDay]
	if !ok {
		return BucketPlan{}, false
	}
	tradingDay := bar.DataTime.Format("2006-01-02")
	cross := minutes == 30 || minutes == 60
	bk := buildBucketKey(tradingDay, meta, minutes, cross)
	expected := expectedMinutesForBucket(minutes, bk, cross, minuteOrder, sessionMinutes)
	if expected <= 0 {
		return BucketPlan{}, false
	}
	labelMinute := bucketLabelMinute(minutes, bk, cross, sortedSessions, minuteOrder)
	if labelMinute < 0 {
		return BucketPlan{}, false
	}
	dataLabel := labelTime(bar.DataTime, labelMinute)
	adjustedBase := bar.AdjustedTime
	if adjustedBase.IsZero() {
		adjustedBase = bar.DataTime
	}
	adjustedLabel := labelTime(adjustedBase, labelMinute)
	if !cross {
		if sessionEnd := sessionEndForMinute(minuteOfDay, sortedSessions); sessionEnd >= 0 {
			endDataLabel := labelTime(bar.DataTime, sessionEnd)
			endAdjustedLabel := labelTime(adjustedBase, sessionEnd)
			if dataLabel.After(endDataLabel) {
				dataLabel = endDataLabel
				adjustedLabel = endAdjustedLabel
			}
		}
	}
	return BucketPlan{
		Key:             renderBucketKey(bk),
		TradingDay:      tradingDay,
		Period:          period,
		DataTime:        dataLabel,
		AdjustedTime:    adjustedLabel,
		ExpectedMinutes: expected,
		CrossSession:    cross,
	}, true
}

func planDailyBucket(bar MinuteBar, sessions []SessionRange) (BucketPlan, bool) {
	if bar.DataTime.IsZero() || len(sessions) == 0 {
		return BucketPlan{}, false
	}
	sortedSessions := append([]SessionRange(nil), sessions...)
	sort.Slice(sortedSessions, func(i, j int) bool {
		return tradingMinuteOrderKey(sortedSessions[i].Start) < tradingMinuteOrderKey(sortedSessions[j].Start)
	})
	start := sortedSessions[0].Start
	if start < 0 {
		start = 0
	}
	tradingDay := bar.DataTime.Format("2006-01-02")
	dataLabel := time.Date(
		bar.DataTime.Year(),
		bar.DataTime.Month(),
		bar.DataTime.Day(),
		start/60,
		start%60,
		0,
		0,
		time.Local,
	)
	adjustedBase := bar.AdjustedTime
	if adjustedBase.IsZero() {
		adjustedBase = bar.DataTime
	}
	adjustedLabel := time.Date(
		adjustedBase.Year(),
		adjustedBase.Month(),
		adjustedBase.Day(),
		start/60,
		start%60,
		0,
		0,
		time.Local,
	)
	minuteOrder, _, _ := buildSessionMinuteMaps(sortedSessions)
	if len(minuteOrder) == 0 {
		return BucketPlan{}, false
	}
	return BucketPlan{
		Key:             tradingDay + "|daily",
		TradingDay:      tradingDay,
		Period:          "1d",
		DataTime:        dataLabel,
		AdjustedTime:    adjustedLabel,
		ExpectedMinutes: len(minuteOrder),
		CrossSession:    true,
	}, true
}
