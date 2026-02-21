package chartlayout

import (
	"encoding/json"
	"time"
)

type DrawingPoint struct {
	Time  int64    `json:"time"`
	Price *float64 `json:"price,omitempty"`
}

type DrawingStyle struct {
	Color   string   `json:"color"`
	Width   *float64 `json:"width,omitempty"`
	Fill    string   `json:"fill,omitempty"`
	Opacity *float64 `json:"opacity,omitempty"`
}

type DrawingObject struct {
	ID           string         `json:"id"`
	Type         string         `json:"type"`
	Owner        string         `json:"owner"`
	Symbol       string         `json:"symbol"`
	Kind         string         `json:"type_kind"`
	Variety      string         `json:"variety"`
	Timeframe    string         `json:"timeframe"`
	ObjectClass  string         `json:"object_class,omitempty"`
	Points       []DrawingPoint `json:"points"`
	Text         string         `json:"text,omitempty"`
	Style        DrawingStyle   `json:"style"`
	StartTime    int64          `json:"start_time,omitempty"`
	EndTime      int64          `json:"end_time,omitempty"`
	StartPrice   *float64       `json:"start_price,omitempty"`
	EndPrice     *float64       `json:"end_price,omitempty"`
	LineColor    string         `json:"line_color,omitempty"`
	LineWidth    *float64       `json:"line_width,omitempty"`
	LineStyle    string         `json:"line_style,omitempty"`
	LeftCap      string         `json:"left_cap,omitempty"`
	RightCap     string         `json:"right_cap,omitempty"`
	LabelText    string         `json:"label_text,omitempty"`
	LabelPos     string         `json:"label_pos,omitempty"`
	LabelAlign   string         `json:"label_align,omitempty"`
	VisibleRange string         `json:"visible_range,omitempty"`
	Locked       bool           `json:"locked"`
	Visible      bool           `json:"visible"`
	Z            int64          `json:"z"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

type PaneSettings struct {
	RightWatchlistOpen bool `json:"right_watchlist_open"`
	BottomPanelOpen    bool `json:"bottom_panel_open"`
}

type IndicatorSettings struct {
	MA20   bool `json:"ma20"`
	MACD   bool `json:"macd"`
	Volume bool `json:"volume"`
}

type ChannelDisplaySettings struct {
	ShowExtrema    bool `json:"show_extrema"`
	ShowRansac     bool `json:"show_ransac"`
	ShowRegression bool `json:"show_regression"`
}

type ChannelCommonSettings struct {
	WindowSizeMinute int     `json:"window_size_minute"`
	WindowSizeHour   int     `json:"window_size_hour"`
	WindowSizeDay    int     `json:"window_size_day"`
	StepDivisor      int     `json:"step_divisor"`
	InsideRatioMin   float64 `json:"inside_ratio_min"`
	MinTouches       int     `json:"min_touches"`
	NmsIoU           float64 `json:"nms_iou"`
	NmsSlopeFactor   float64 `json:"nms_slope_factor"`
	MaxSegments      int     `json:"max_segments"`
	ShowTopAutoN     int     `json:"show_top_auto_n"`
	HideAuto         bool    `json:"hide_auto"`
	LiveApply        bool    `json:"live_apply"`
	ShowLabels       bool    `json:"show_labels"`
}

type ChannelExtremaSettings struct {
	// PivotK* 用于控制不同周期下“局部极值点”识别的灵敏度。
	// 运行时按当前图表周期选择参数：
	// - 分钟级（如 1m/5m/15m/30m）使用 PivotKMinute
	// - 小时级（如 1h/2h/4h）使用 PivotKHour
	// - 日级及以上（如 1d/1w）使用 PivotKDay
	// K 越大，极值识别越严格（极值点更少，通道更平滑）。
	PivotKMinute            int     `json:"pivot_k_minute"`
	PivotKHour              int     `json:"pivot_k_hour"`
	PivotKDay               int     `json:"pivot_k_day"`
	// MinPairSpanBars/MaxPairSpanBars 用于限制两端锚点极值之间的 K 线跨度（bar 数）。
	// 跨度过小容易噪声化，跨度过大可能连接到不相关波段。
	MinPairSpanBars         int     `json:"min_pair_span_bars"`
	MaxPairSpanBars         int     `json:"max_pair_span_bars"`
	// SecondPointAtrFactor 表示第二个锚点与首锚点的最小有效分离阈值（以 ATR 为单位）。
	// 用于过滤“过近的重复极值点”，避免形成无意义连线。
	SecondPointAtrFactor    float64 `json:"second_point_atr_factor"`
	// BreakToleranceAtrFactor 表示通道约束校验时允许的边界越界容差（以 ATR 为单位）。
	// 数值越大，对边界附近的噪声/假突破容忍度越高。
	BreakToleranceAtrFactor float64 `json:"break_tolerance_atr_factor"`
}

type ChannelRansacSettings struct {
	PivotKMinute      int     `json:"pivot_k_minute"`
	PivotKHour        int     `json:"pivot_k_hour"`
	PivotKDay         int     `json:"pivot_k_day"`
	ResidualAtrFactor float64 `json:"residual_atr_factor"`
	SlopeTolAtrFactor float64 `json:"slope_tol_atr_factor"`
}

type ChannelRegressionSettings struct {
	ResidualAtrFactor float64 `json:"residual_atr_factor"`
}

type ChannelAlgorithmSettings struct {
	Extrema    ChannelExtremaSettings    `json:"extrema"`
	Ransac     ChannelRansacSettings     `json:"ransac"`
	Regression ChannelRegressionSettings `json:"regression"`
}

type ChannelSettings struct {
	Display    ChannelDisplaySettings   `json:"display"`
	Common     ChannelCommonSettings    `json:"common"`
	Algorithms ChannelAlgorithmSettings `json:"algorithms"`
}

func DefaultChannelSettings() ChannelSettings {
	return ChannelSettings{
		Display: ChannelDisplaySettings{
			ShowExtrema:    true,
			ShowRansac:     false,
			ShowRegression: false,
		},
		Common: ChannelCommonSettings{
			WindowSizeMinute: 120,
			WindowSizeHour:   160,
			WindowSizeDay:    200,
			StepDivisor:      4,
			InsideRatioMin:   0.78,
			MinTouches:       2,
			NmsIoU:           0.6,
			NmsSlopeFactor:   1.5,
			MaxSegments:      6,
			ShowTopAutoN:     2,
			HideAuto:         false,
			LiveApply:        true,
			ShowLabels:       true,
		},
		Algorithms: ChannelAlgorithmSettings{
			Extrema: ChannelExtremaSettings{
				PivotKMinute:            3,
				PivotKHour:              5,
				PivotKDay:               8,
				MinPairSpanBars:         12,
				MaxPairSpanBars:         120,
				SecondPointAtrFactor:    0.25,
				BreakToleranceAtrFactor: 0.3,
			},
			Ransac: ChannelRansacSettings{
				PivotKMinute:      3,
				PivotKHour:        5,
				PivotKDay:         8,
				ResidualAtrFactor: 0.8,
				SlopeTolAtrFactor: 0.12,
			},
			Regression: ChannelRegressionSettings{
				ResidualAtrFactor: 0.8,
			},
		},
	}
}

func (s *ChannelSettings) UnmarshalJSON(data []byte) error {
	type alias ChannelSettings
	def := DefaultChannelSettings()
	*s = def

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	_, hasDisplay := raw["display"]
	_, hasCommon := raw["common"]
	_, hasAlgorithms := raw["algorithms"]

	var grouped alias
	if hasDisplay || hasCommon || hasAlgorithms {
		grouped = alias(*s)
		if err := json.Unmarshal(data, &grouped); err != nil {
			return err
		}
		*s = ChannelSettings(grouped)
		return nil
	}

	var legacy struct {
		WindowSizeMinute  int     `json:"window_size_minute"`
		WindowSizeHour    int     `json:"window_size_hour"`
		WindowSizeDay     int     `json:"window_size_day"`
		StepDivisor       int     `json:"step_divisor"`
		PivotKMinute      int     `json:"pivot_k_minute"`
		PivotKHour        int     `json:"pivot_k_hour"`
		PivotKDay         int     `json:"pivot_k_day"`
		ResidualAtrFactor float64 `json:"residual_atr_factor"`
		SlopeTolAtrFactor float64 `json:"slope_tol_atr_factor"`
		InsideRatioMin    float64 `json:"inside_ratio_min"`
		MinTouches        int     `json:"min_touches"`
		NmsIoU            float64 `json:"nms_iou"`
		NmsSlopeFactor    float64 `json:"nms_slope_factor"`
		MaxSegments       int     `json:"max_segments"`
		ShowTopAutoN      int     `json:"show_top_auto_n"`
		HideAuto          bool    `json:"hide_auto"`
		LiveApply         bool    `json:"live_apply"`
		ShowLabels        bool    `json:"show_labels"`
	}
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}
	if legacy.WindowSizeMinute > 0 {
		s.Common.WindowSizeMinute = legacy.WindowSizeMinute
	}
	if legacy.WindowSizeHour > 0 {
		s.Common.WindowSizeHour = legacy.WindowSizeHour
	}
	if legacy.WindowSizeDay > 0 {
		s.Common.WindowSizeDay = legacy.WindowSizeDay
	}
	if legacy.StepDivisor > 0 {
		s.Common.StepDivisor = legacy.StepDivisor
	}
	if legacy.InsideRatioMin > 0 {
		s.Common.InsideRatioMin = legacy.InsideRatioMin
	}
	if legacy.MinTouches > 0 {
		s.Common.MinTouches = legacy.MinTouches
	}
	if legacy.NmsIoU > 0 {
		s.Common.NmsIoU = legacy.NmsIoU
	}
	if legacy.NmsSlopeFactor > 0 {
		s.Common.NmsSlopeFactor = legacy.NmsSlopeFactor
	}
	if legacy.MaxSegments > 0 {
		s.Common.MaxSegments = legacy.MaxSegments
	}
	if _, ok := raw["show_top_auto_n"]; ok {
		s.Common.ShowTopAutoN = legacy.ShowTopAutoN
	}
	if _, ok := raw["showTopAutoN"]; ok {
		s.Common.ShowTopAutoN = legacy.ShowTopAutoN
	}
	if _, ok := raw["hide_auto"]; ok {
		s.Common.HideAuto = legacy.HideAuto
	}
	if _, ok := raw["hideAuto"]; ok {
		s.Common.HideAuto = legacy.HideAuto
	}
	if _, ok := raw["live_apply"]; ok {
		s.Common.LiveApply = legacy.LiveApply
	}
	if _, ok := raw["liveApply"]; ok {
		s.Common.LiveApply = legacy.LiveApply
	}
	if _, ok := raw["show_labels"]; ok {
		s.Common.ShowLabels = legacy.ShowLabels
	}
	if _, ok := raw["showLabels"]; ok {
		s.Common.ShowLabels = legacy.ShowLabels
	}
	if legacy.PivotKMinute > 0 {
		s.Algorithms.Ransac.PivotKMinute = legacy.PivotKMinute
		s.Algorithms.Extrema.PivotKMinute = legacy.PivotKMinute
	}
	if legacy.PivotKHour > 0 {
		s.Algorithms.Ransac.PivotKHour = legacy.PivotKHour
		s.Algorithms.Extrema.PivotKHour = legacy.PivotKHour
	}
	if legacy.PivotKDay > 0 {
		s.Algorithms.Ransac.PivotKDay = legacy.PivotKDay
		s.Algorithms.Extrema.PivotKDay = legacy.PivotKDay
	}
	if legacy.ResidualAtrFactor > 0 {
		s.Algorithms.Ransac.ResidualAtrFactor = legacy.ResidualAtrFactor
		s.Algorithms.Regression.ResidualAtrFactor = legacy.ResidualAtrFactor
	}
	if legacy.SlopeTolAtrFactor > 0 {
		s.Algorithms.Ransac.SlopeTolAtrFactor = legacy.SlopeTolAtrFactor
	}
	return nil
}

type ChannelDecision struct {
	ID         string    `json:"id"`
	BaseID     string    `json:"base_id,omitempty"`
	Status     string    `json:"status"`
	Locked     bool      `json:"locked"`
	Hidden     bool      `json:"hidden"`
	Method     string    `json:"method,omitempty"`
	StartTime  int64     `json:"start_time,omitempty"`
	EndTime    int64     `json:"end_time,omitempty"`
	UpperStart float64   `json:"upper_start,omitempty"`
	UpperEnd   float64   `json:"upper_end,omitempty"`
	LowerStart float64   `json:"lower_start,omitempty"`
	LowerEnd   float64   `json:"lower_end,omitempty"`
	Slope      float64   `json:"slope,omitempty"`
	Score      float64   `json:"score,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
	Operator   string    `json:"operator,omitempty"`
	Source     string    `json:"source,omitempty"`
}

type ChannelLayout struct {
	Settings   ChannelSettings   `json:"settings"`
	Decisions  []ChannelDecision `json:"decisions"`
	SelectedID string            `json:"selected_id,omitempty"`
}

type LayoutSnapshot struct {
	Owner      string            `json:"owner"`
	Symbol     string            `json:"symbol"`
	Type       string            `json:"type"`
	Variety    string            `json:"variety"`
	Timeframe  string            `json:"timeframe"`
	Theme      string            `json:"theme"`
	Panes      PaneSettings      `json:"panes"`
	Indicators IndicatorSettings `json:"indicators"`
	Channels   ChannelLayout     `json:"channels"`
	Drawings   []DrawingObject   `json:"drawings"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

type Scope struct {
	Owner     string
	Symbol    string
	Kind      string
	Variety   string
	Timeframe string
}
