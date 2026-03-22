package chartlayout

import (
	"encoding/json"
	"time"
)

type DrawingPoint struct {
	// Time 是绘图锚点对应的时间戳。
	Time int64 `json:"time"`
	// Price 是绘图锚点对应的价格，允许为空表示纯时间点。
	Price *float64 `json:"price,omitempty"`
}

type DrawingStyle struct {
	// Color 是主颜色。
	Color string `json:"color"`
	// Width 是线宽。
	Width *float64 `json:"width,omitempty"`
	// Fill 是填充色。
	Fill string `json:"fill,omitempty"`
	// Opacity 是透明度。
	Opacity *float64 `json:"opacity,omitempty"`
}

type DrawingObject struct {
	// ID 是绘图对象唯一标识。
	ID string `json:"id"`
	// Type 是绘图类型，例如 line、ray、rect。
	Type string `json:"type"`
	// Owner 是布局归属者。
	Owner string `json:"owner"`
	// Symbol 是绘图所属 symbol。
	Symbol string `json:"symbol"`
	// Kind 是绘图所属数据类型。
	Kind string `json:"type_kind"`
	// Variety 是品种代码。
	Variety string `json:"variety"`
	// Timeframe 是所属周期。
	Timeframe string `json:"timeframe"`
	// ObjectClass 是附加对象分类。
	ObjectClass string `json:"object_class,omitempty"`
	// Points 是绘图锚点集合。
	Points []DrawingPoint `json:"points"`
	// Text 是文本类绘图内容。
	Text string `json:"text,omitempty"`
	// Style 是通用样式配置。
	Style DrawingStyle `json:"style"`
	// StartTime 是线段或区域起始时间。
	StartTime int64 `json:"start_time,omitempty"`
	// EndTime 是线段或区域结束时间。
	EndTime int64 `json:"end_time,omitempty"`
	// StartPrice 是起始价格。
	StartPrice *float64 `json:"start_price,omitempty"`
	// EndPrice 是结束价格。
	EndPrice *float64 `json:"end_price,omitempty"`
	// LineColor 是线条颜色覆盖值。
	LineColor string `json:"line_color,omitempty"`
	// LineWidth 是线条宽度覆盖值。
	LineWidth *float64 `json:"line_width,omitempty"`
	// LineStyle 是线型。
	LineStyle string `json:"line_style,omitempty"`
	// LeftCap 是左端点样式。
	LeftCap string `json:"left_cap,omitempty"`
	// RightCap 是右端点样式。
	RightCap string `json:"right_cap,omitempty"`
	// LabelText 是对象标签文本。
	LabelText string `json:"label_text,omitempty"`
	// LabelPos 是标签位置。
	LabelPos string `json:"label_pos,omitempty"`
	// LabelAlign 是标签对齐方式。
	LabelAlign string `json:"label_align,omitempty"`
	// VisibleRange 指定对象的可见范围策略。
	VisibleRange string `json:"visible_range,omitempty"`
	// Locked 表示对象是否禁止编辑。
	Locked bool `json:"locked"`
	// Visible 表示对象是否显示。
	Visible bool `json:"visible"`
	// Z 是绘图层级。
	Z int64 `json:"z"`
	// CreatedAt 是对象创建时间。
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt 是对象最后更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

type PaneSettings struct {
	// RightWatchlistOpen 表示右侧观察列表是否展开。
	RightWatchlistOpen bool `json:"right_watchlist_open"`
	// BottomPanelOpen 表示底部面板是否展开。
	BottomPanelOpen bool `json:"bottom_panel_open"`
}

type IndicatorSettings struct {
	// MA20 控制是否显示 MA20。
	MA20 bool `json:"ma20"`
	// MACD 控制是否显示 MACD。
	MACD bool `json:"macd"`
	// Volume 控制是否显示成交量。
	Volume bool `json:"volume"`
}

type ChannelDisplaySettings struct {
	// ShowExtrema 控制是否显示极值法通道。
	ShowExtrema bool `json:"show_extrema"`
	// ShowRansac 控制是否显示 RANSAC 通道。
	ShowRansac bool `json:"show_ransac"`
	// ShowRegression 控制是否显示回归通道。
	ShowRegression bool `json:"show_regression"`
}

type ChannelCommonSettings struct {
	// WindowSizeMinute 是分钟级窗口大小。
	WindowSizeMinute int `json:"window_size_minute"`
	// WindowSizeHour 是小时级窗口大小。
	WindowSizeHour int `json:"window_size_hour"`
	// WindowSizeDay 是日级窗口大小。
	WindowSizeDay int `json:"window_size_day"`
	// StepDivisor 控制窗口滑动步长分母。
	StepDivisor int `json:"step_divisor"`
	// InsideRatioMin 是通道内部点占比阈值。
	InsideRatioMin float64 `json:"inside_ratio_min"`
	// MinTouches 是有效通道的最小触碰次数。
	MinTouches int `json:"min_touches"`
	// NmsIoU 是通道去重时的 IoU 阈值。
	NmsIoU float64 `json:"nms_iou"`
	// NmsSlopeFactor 是通道去重时斜率相似度阈值。
	NmsSlopeFactor float64 `json:"nms_slope_factor"`
	// MaxSegments 是每次最多保留的通道段数。
	MaxSegments int `json:"max_segments"`
	// ShowTopAutoN 控制自动展示前 N 个候选通道。
	ShowTopAutoN int `json:"show_top_auto_n"`
	// HideAuto 控制是否隐藏自动通道。
	HideAuto bool `json:"hide_auto"`
	// LiveApply 控制实时计算时是否自动应用通道结果。
	LiveApply bool `json:"live_apply"`
	// ShowLabels 控制是否显示通道标签。
	ShowLabels bool `json:"show_labels"`
}

type ChannelExtremaSettings struct {
	// PivotK* 用于控制不同周期下“局部极值点”识别的灵敏度。
	// 运行时按当前图表周期选择参数：
	// - 分钟级（如 1m/5m/15m/30m）使用 PivotKMinute
	// - 小时级（如 1h/2h/4h）使用 PivotKHour
	// - 日级及以上（如 1d/1w）使用 PivotKDay
	// K 越大，极值识别越严格（极值点更少，通道更平滑）。
	PivotKMinute int `json:"pivot_k_minute"`
	PivotKHour   int `json:"pivot_k_hour"`
	PivotKDay    int `json:"pivot_k_day"`
	// MinPairSpanBars/MaxPairSpanBars 用于限制两端锚点极值之间的 K 线跨度（bar 数）。
	// 跨度过小容易噪声化，跨度过大可能连接到不相关波段。
	MinPairSpanBars int `json:"min_pair_span_bars"`
	MaxPairSpanBars int `json:"max_pair_span_bars"`
	// SecondPointAtrFactor 表示第二个锚点与首锚点的最小有效分离阈值（以 ATR 为单位）。
	// 用于过滤“过近的重复极值点”，避免形成无意义连线。
	SecondPointAtrFactor float64 `json:"second_point_atr_factor"`
	// BreakToleranceAtrFactor 表示通道约束校验时允许的边界越界容差（以 ATR 为单位）。
	// 数值越大，对边界附近的噪声/假突破容忍度越高。
	BreakToleranceAtrFactor float64 `json:"break_tolerance_atr_factor"`
}

type ChannelRansacSettings struct {
	// PivotKMinute 是分钟级拐点识别参数。
	PivotKMinute int `json:"pivot_k_minute"`
	// PivotKHour 是小时级拐点识别参数。
	PivotKHour int `json:"pivot_k_hour"`
	// PivotKDay 是日级拐点识别参数。
	PivotKDay int `json:"pivot_k_day"`
	// ResidualAtrFactor 是残差容忍 ATR 系数。
	ResidualAtrFactor float64 `json:"residual_atr_factor"`
	// SlopeTolAtrFactor 是斜率容忍 ATR 系数。
	SlopeTolAtrFactor float64 `json:"slope_tol_atr_factor"`
}

type ChannelRegressionSettings struct {
	// ResidualAtrFactor 是回归通道残差容忍 ATR 系数。
	ResidualAtrFactor float64 `json:"residual_atr_factor"`
}

type ChannelAlgorithmSettings struct {
	// Extrema 是极值法参数。
	Extrema ChannelExtremaSettings `json:"extrema"`
	// Ransac 是 RANSAC 法参数。
	Ransac ChannelRansacSettings `json:"ransac"`
	// Regression 是回归法参数。
	Regression ChannelRegressionSettings `json:"regression"`
}

type ChannelSettings struct {
	// Display 控制通道图层显示。
	Display ChannelDisplaySettings `json:"display"`
	// Common 是通道算法共享参数。
	Common ChannelCommonSettings `json:"common"`
	// Algorithms 是各类通道算法的专属参数。
	Algorithms ChannelAlgorithmSettings `json:"algorithms"`
}

func DefaultChannelSettings() ChannelSettings {
	return ChannelSettings{
		Display: ChannelDisplaySettings{
			ShowExtrema:    false,
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
	// ID 是通道决策唯一标识。
	ID string `json:"id"`
	// BaseID 是关联的基础通道 ID。
	BaseID string `json:"base_id,omitempty"`
	// Status 是当前决策状态。
	Status string `json:"status"`
	// Locked 表示该通道是否被锁定。
	Locked bool `json:"locked"`
	// Hidden 表示该通道是否隐藏。
	Hidden bool `json:"hidden"`
	// Method 是该通道来源算法。
	Method string `json:"method,omitempty"`
	// StartTime 是上轨起始时间。
	StartTime int64 `json:"start_time,omitempty"`
	// EndTime 是上轨结束时间。
	EndTime int64 `json:"end_time,omitempty"`
	// UpperStart 是上轨起点价格。
	UpperStart float64 `json:"upper_start,omitempty"`
	// UpperEnd 是上轨终点价格。
	UpperEnd float64 `json:"upper_end,omitempty"`
	// LowerStart 是下轨起点价格。
	LowerStart float64 `json:"lower_start,omitempty"`
	// LowerEnd 是下轨终点价格。
	LowerEnd float64 `json:"lower_end,omitempty"`
	// Slope 是通道斜率。
	Slope float64 `json:"slope,omitempty"`
	// Score 是通道评分。
	Score float64 `json:"score,omitempty"`
	// UpdatedAt 是决策更新时间。
	UpdatedAt time.Time `json:"updated_at"`
	// Operator 是最近一次操作人。
	Operator string `json:"operator,omitempty"`
	// Source 标记决策来源。
	Source string `json:"source,omitempty"`
}

type ChannelLayout struct {
	// Settings 是通道配置。
	Settings ChannelSettings `json:"settings"`
	// Decisions 是当前所有通道决策。
	Decisions []ChannelDecision `json:"decisions"`
	// SelectedID 是当前选中的通道 ID。
	SelectedID string `json:"selected_id,omitempty"`
}

type ReversalSettings struct {
	// Enabled 控制是否启用反转形态识别。
	Enabled bool `json:"enabled"`
	// MidTrendMinBars 是中继趋势最小 bar 数。
	MidTrendMinBars int `json:"mid_trend_min_bars"`
	// MidTrendMaxBars 是中继趋势最大 bar 数。
	MidTrendMaxBars int `json:"mid_trend_max_bars"`
	// PivotKMinute 是分钟级拐点识别参数。
	PivotKMinute int `json:"pivot_k_minute"`
	// PivotKHour 是小时级拐点识别参数。
	PivotKHour int `json:"pivot_k_hour"`
	// PivotKDay 是日级拐点识别参数。
	PivotKDay int `json:"pivot_k_day"`
	// LineToleranceAtr 是连线容忍 ATR 系数。
	LineToleranceAtr float64 `json:"line_tolerance_atr_factor"`
	// BreakThresholdPct 是突破阈值百分比。
	BreakThresholdPct float64 `json:"break_threshold_pct"`
	// MinSwingAmplitudeAtr 是最小摆动幅度 ATR 阈值。
	MinSwingAmplitudeAtr float64 `json:"min_swing_amplitude_atr"`
	// ConfirmOnClose 控制是否以收盘确认突破。
	ConfirmOnClose bool `json:"confirm_on_close"`
	// ShowLabels 控制是否显示形态标签。
	ShowLabels bool `json:"show_labels"`
}

type ReversalLine struct {
	// ID 是反转线唯一标识。
	ID string `json:"id"`
	// Side 表示该线属于压力线还是支撑线。
	Side string `json:"side"`
	// StartIndex 是起点在 bars 中的索引。
	StartIndex int `json:"start_index"`
	// EndIndex 是终点在 bars 中的索引。
	EndIndex int `json:"end_index"`
	// StartPrice 是起点价格。
	StartPrice float64 `json:"start_price"`
	// EndPrice 是终点价格。
	EndPrice float64 `json:"end_price"`
	// Slope 是线条斜率。
	Slope float64 `json:"slope"`
	// Intercept 是线性表达式截距。
	Intercept float64 `json:"intercept"`
	// Touches 是触碰次数。
	Touches int `json:"touches"`
	// Score 是线条评分。
	Score float64 `json:"score"`
}

type ReversalPoint struct {
	// Index 是点在 bars 中的索引。
	Index int64 `json:"index"`
	// Time 是点的时间戳。
	Time int64 `json:"time"`
	// Price 是点的价格。
	Price float64 `json:"price"`
}

type ReversalCondition struct {
	// Met 表示条件是否满足。
	Met bool `json:"met"`
	// At 是条件满足的时间戳。
	At int64 `json:"at"`
}

type ReversalEvent struct {
	// ID 是事件唯一标识。
	ID string `json:"id"`
	// LineID 是事件关联的趋势线 ID。
	LineID string `json:"line_id"`
	// Direction 表示反转方向。
	Direction string `json:"direction"`
	// P1 是第一关键点。
	P1 *ReversalPoint `json:"p1,omitempty"`
	// P2 是第二关键点。
	P2 *ReversalPoint `json:"p2,omitempty"`
	// P3 是第三关键点。
	P3 *ReversalPoint `json:"p3,omitempty"`
	// Conditions 是各类判定条件及结果。
	Conditions map[string]ReversalCondition `json:"conditions,omitempty"`
	// Confirmed 表示事件是否已确认。
	Confirmed bool `json:"confirmed"`
	// Invalidated 表示事件是否已失效。
	Invalidated bool `json:"invalidated"`
	// Score 是事件评分。
	Score float64 `json:"score"`
	// Label 是前端展示标签。
	Label string `json:"label,omitempty"`
}

type ReversalResult struct {
	// Lines 是识别出的候选趋势线。
	Lines []ReversalLine `json:"lines"`
	// Events 是识别出的反转事件。
	Events []ReversalEvent `json:"events"`
}

type ReversalLayout struct {
	// Settings 是反转识别配置。
	Settings ReversalSettings `json:"settings"`
	// Results 是当前反转识别结果。
	Results ReversalResult `json:"results"`
	// PersistVersion 是持久化版本号，用于并发控制。
	PersistVersion int64 `json:"persist_version"`
	// SelectedID 是当前选中的事件或线条 ID。
	SelectedID string `json:"selected_id,omitempty"`
}

func DefaultReversalSettings() ReversalSettings {
	return ReversalSettings{
		Enabled:              false,
		MidTrendMinBars:      50,
		MidTrendMaxBars:      2000,
		PivotKMinute:         3,
		PivotKHour:           5,
		PivotKDay:            8,
		LineToleranceAtr:     1.0,
		BreakThresholdPct:    3.0,
		MinSwingAmplitudeAtr: 1.0,
		ConfirmOnClose:       true,
		ShowLabels:           true,
	}
}

type LayoutSnapshot struct {
	// Owner 是布局归属者。
	Owner string `json:"owner"`
	// Symbol 是布局对应 symbol。
	Symbol string `json:"symbol"`
	// Type 是布局对应的数据类型。
	Type string `json:"type"`
	// Variety 是品种代码。
	Variety string `json:"variety"`
	// Timeframe 是周期。
	Timeframe string `json:"timeframe"`
	// Theme 是当前主题名称。
	Theme string `json:"theme"`
	// Panes 是面板开关设置。
	Panes PaneSettings `json:"panes"`
	// Indicators 是指标显示设置。
	Indicators IndicatorSettings `json:"indicators"`
	// Channels 是通道布局设置和结果。
	Channels ChannelLayout `json:"channels"`
	// Reversal 是反转布局设置和结果。
	Reversal ReversalLayout `json:"reversal"`
	// Drawings 是自定义绘图对象列表。
	Drawings []DrawingObject `json:"drawings"`
	// UpdatedAt 是布局快照更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

type Scope struct {
	// Owner 是布局归属者。
	Owner string
	// Symbol 是目标 symbol。
	Symbol string
	// Kind 是目标类型。
	Kind string
	// Variety 是品种代码。
	Variety string
	// Timeframe 是周期。
	Timeframe string
}
