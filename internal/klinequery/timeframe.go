package klinequery

import "strings"

func normalizeTimeframe(raw string) (string, int, error) {
	tf := strings.ToLower(strings.TrimSpace(raw))
	if tf == "" {
		tf = "1m"
	}
	switch tf {
	case "1m":
		return tf, 1, nil
	case "5m":
		return tf, 5, nil
	case "15m":
		return tf, 15, nil
	case "30m":
		return tf, 30, nil
	case "1h":
		return tf, 60, nil
	case "1d":
		return tf, 1440, nil
	default:
		return "", 0, newInvalidTimeframeError(raw)
	}
}
