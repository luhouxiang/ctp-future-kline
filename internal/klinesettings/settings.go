package klinesettings

import "strings"

var SupportedTimeframes = []string{"1m", "5m", "15m", "30m", "1h", "1d"}

type Settings struct {
	Contract map[string]bool `json:"contract"`
	L9       map[string]bool `json:"l9"`
}

func Default() Settings {
	all := make(map[string]bool, len(SupportedTimeframes))
	for _, tf := range SupportedTimeframes {
		all[tf] = true
	}
	contract := make(map[string]bool, len(all))
	l9 := make(map[string]bool, len(all))
	for k, v := range all {
		contract[k] = v
		l9[k] = v
	}
	return Settings{
		Contract: contract,
		L9:       l9,
	}
}

func Normalize(in Settings) Settings {
	out := Default()
	mergeKind := func(dst map[string]bool, src map[string]bool) {
		if src == nil {
			return
		}
		for _, tf := range SupportedTimeframes {
			v, ok := src[tf]
			if ok {
				dst[tf] = v
			}
		}
	}
	mergeKind(out.Contract, in.Contract)
	mergeKind(out.L9, in.L9)
	return out
}

func (s Settings) Enabled(kind string, timeframe string) bool {
	tf := normalizeTimeframe(timeframe)
	if tf == "" {
		return false
	}
	n := Normalize(s)
	switch normalizeKind(kind) {
	case "l9":
		return n.L9[tf]
	default:
		return n.Contract[tf]
	}
}

func (s Settings) AnyEnabled(kind string) bool {
	n := Normalize(s)
	var src map[string]bool
	if normalizeKind(kind) == "l9" {
		src = n.L9
	} else {
		src = n.Contract
	}
	for _, tf := range SupportedTimeframes {
		if src[tf] {
			return true
		}
	}
	return false
}

func (s Settings) AnyHigherEnabled(kind string) bool {
	n := Normalize(s)
	var src map[string]bool
	if normalizeKind(kind) == "l9" {
		src = n.L9
	} else {
		src = n.Contract
	}
	for _, tf := range []string{"5m", "15m", "30m", "1h", "1d"} {
		if src[tf] {
			return true
		}
	}
	return false
}

func normalizeKind(v string) string {
	if strings.EqualFold(strings.TrimSpace(v), "l9") {
		return "l9"
	}
	return "contract"
}

func normalizeTimeframe(v string) string {
	t := strings.ToLower(strings.TrimSpace(v))
	switch t {
	case "1m", "5m", "15m", "30m", "1h", "1d":
		return t
	default:
		return ""
	}
}
