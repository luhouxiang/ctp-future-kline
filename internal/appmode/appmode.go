package appmode

import "strings"

const (
	LiveReal    = "live_real"
	LivePaper   = "live_paper"
	ReplayPaper = "replay_paper"
)

func Normalize(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case LivePaper:
		return LivePaper
	case ReplayPaper:
		return ReplayPaper
	default:
		return LiveReal
	}
}

func IsReplay(raw string) bool {
	return Normalize(raw) == ReplayPaper
}

func UsesRealtimeMarket(raw string) bool {
	return Normalize(raw) != ReplayPaper
}
