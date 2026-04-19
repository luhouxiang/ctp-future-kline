package quotes

import (
	"sync"

	"ctp-future-kline/internal/klinesettings"
)

var (
	klineGenerationSettingsMu sync.RWMutex
	klineGenerationSettings   = klinesettings.Default()
)

func SetKlineGenerationSettings(settings klinesettings.Settings) {
	klineGenerationSettingsMu.Lock()
	klineGenerationSettings = klinesettings.Normalize(settings)
	klineGenerationSettingsMu.Unlock()
}

func GetKlineGenerationSettings() klinesettings.Settings {
	klineGenerationSettingsMu.RLock()
	defer klineGenerationSettingsMu.RUnlock()
	return klinesettings.Normalize(klineGenerationSettings)
}
