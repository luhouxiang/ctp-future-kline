package klinequery

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidTimeframe       = errors.New("invalid timeframe")
	ErrTradingSessionNotReady = errors.New("trading session not completed")
)

func newInvalidTimeframeError(v string) error {
	return fmt.Errorf("%w: unsupported timeframe %q, supported: 1m/5m/15m/30m/1h/1d", ErrInvalidTimeframe, v)
}

func newTradingSessionNotReadyError() error {
	return fmt.Errorf("%w: 交易时段未完成，请先建立并完成该品种交易时段", ErrTradingSessionNotReady)
}
