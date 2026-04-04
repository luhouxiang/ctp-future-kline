package queuewatch

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/logger"
)

const (
	AlertLevelNormal    = "normal"
	AlertLevelWarn      = "warn"
	AlertLevelCritical  = "critical"
	AlertLevelEmergency = "emergency"
)

const (
	defaultWarnPercent      = 60
	defaultCriticalPercent  = 80
	defaultEmergencyPercent = 95
	defaultRecoverPercent   = 50

	defaultShardCapacity            = 8192
	defaultPersistCapacity          = 16384
	defaultMMDeferredCapacity       = 16384
	defaultL9TaskCapacity           = 4096
	defaultFilePerShardCapacity     = 1025
	defaultSideEffectTickCapacity   = 16384
	defaultSideEffectBarCapacity    = 4096
	defaultChartSubscriberCapacity  = 4096
	defaultStatusSubscriberCapacity = 16
	defaultStrategyEventCapacity    = 32
	defaultTradeEventCapacity       = 64
	defaultTradeGatewayCapacity     = 32
	defaultMDDisconnectCapacity     = 16
)

type Config struct {
	SpoolDir string

	WarnPercent      int
	CriticalPercent  int
	EmergencyPercent int
	RecoverPercent   int

	ShardCapacity             int
	PersistCapacity           int
	MMDeferredCapacity        int
	L9TaskCapacity            int
	FilePerShardCapacity      int
	SideEffectTickCapacity    int
	SideEffectBarCapacity     int
	ChartSubscriberCapacity   int
	StatusSubscriberCapacity  int
	StrategyEventCapacity     int
	TradeEventCapacity        int
	TradeGatewayEventCapacity int
	MDDisconnectCapacity      int
}

func DefaultConfig(flowPath string) Config {
	spoolDir := ""
	flowPath = strings.TrimSpace(flowPath)
	if flowPath != "" {
		spoolDir = filepath.Join(flowPath, "queue_spool")
	}
	return Config{
		SpoolDir:                  spoolDir,
		WarnPercent:               defaultWarnPercent,
		CriticalPercent:           defaultCriticalPercent,
		EmergencyPercent:          defaultEmergencyPercent,
		RecoverPercent:            defaultRecoverPercent,
		ShardCapacity:             defaultShardCapacity,
		PersistCapacity:           defaultPersistCapacity,
		MMDeferredCapacity:        defaultMMDeferredCapacity,
		L9TaskCapacity:            defaultL9TaskCapacity,
		FilePerShardCapacity:      defaultFilePerShardCapacity,
		SideEffectTickCapacity:    defaultSideEffectTickCapacity,
		SideEffectBarCapacity:     defaultSideEffectBarCapacity,
		ChartSubscriberCapacity:   defaultChartSubscriberCapacity,
		StatusSubscriberCapacity:  defaultStatusSubscriberCapacity,
		StrategyEventCapacity:     defaultStrategyEventCapacity,
		TradeEventCapacity:        defaultTradeEventCapacity,
		TradeGatewayEventCapacity: defaultTradeGatewayCapacity,
		MDDisconnectCapacity:      defaultMDDisconnectCapacity,
	}
}

func (c Config) normalized() Config {
	if c.WarnPercent <= 0 {
		c.WarnPercent = defaultWarnPercent
	}
	if c.CriticalPercent <= 0 {
		c.CriticalPercent = defaultCriticalPercent
	}
	if c.EmergencyPercent <= 0 {
		c.EmergencyPercent = defaultEmergencyPercent
	}
	if c.RecoverPercent <= 0 {
		c.RecoverPercent = defaultRecoverPercent
	}
	if c.ShardCapacity <= 0 {
		c.ShardCapacity = defaultShardCapacity
	}
	if c.PersistCapacity <= 0 {
		c.PersistCapacity = defaultPersistCapacity
	}
	if c.MMDeferredCapacity <= 0 {
		c.MMDeferredCapacity = defaultMMDeferredCapacity
	}
	if c.L9TaskCapacity <= 0 {
		c.L9TaskCapacity = defaultL9TaskCapacity
	}
	if c.FilePerShardCapacity <= 0 {
		c.FilePerShardCapacity = defaultFilePerShardCapacity
	}
	if c.SideEffectTickCapacity <= 0 {
		c.SideEffectTickCapacity = defaultSideEffectTickCapacity
	}
	if c.SideEffectBarCapacity <= 0 {
		c.SideEffectBarCapacity = defaultSideEffectBarCapacity
	}
	if c.ChartSubscriberCapacity <= 0 {
		c.ChartSubscriberCapacity = defaultChartSubscriberCapacity
	}
	if c.StatusSubscriberCapacity <= 0 {
		c.StatusSubscriberCapacity = defaultStatusSubscriberCapacity
	}
	if c.StrategyEventCapacity <= 0 {
		c.StrategyEventCapacity = defaultStrategyEventCapacity
	}
	if c.TradeEventCapacity <= 0 {
		c.TradeEventCapacity = defaultTradeEventCapacity
	}
	if c.TradeGatewayEventCapacity <= 0 {
		c.TradeGatewayEventCapacity = defaultTradeGatewayCapacity
	}
	if c.MDDisconnectCapacity <= 0 {
		c.MDDisconnectCapacity = defaultMDDisconnectCapacity
	}
	if c.RecoverPercent >= c.WarnPercent {
		c.RecoverPercent = defaultRecoverPercent
	}
	return c
}

type QueueSpec struct {
	Name        string
	Category    string
	Criticality string
	Capacity    int
	LossPolicy  string
	BasisText   string
}

type QueueSnapshot struct {
	Name          string    `json:"name"`
	Category      string    `json:"category"`
	Criticality   string    `json:"criticality"`
	Capacity      int       `json:"capacity"`
	CurrentDepth  int       `json:"current_depth"`
	UsagePercent  float64   `json:"usage_percent"`
	HighWatermark int       `json:"high_watermark"`
	EnqueueTotal  int64     `json:"enqueue_total"`
	DequeueTotal  int64     `json:"dequeue_total"`
	DropTotal     int64     `json:"drop_total"`
	SpillTotal    int64     `json:"spill_total"`
	SpillBytes    int64     `json:"spill_bytes"`
	SpillDepth    int       `json:"spill_depth"`
	SpillActive   bool      `json:"spill_active"`
	AlertLevel    string    `json:"alert_level"`
	LossPolicy    string    `json:"loss_policy"`
	BasisText     string    `json:"basis_text"`
	LastAlertAt   time.Time `json:"last_alert_at"`
	LastDropAt    time.Time `json:"last_drop_at"`
	LastSpillAt   time.Time `json:"last_spill_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type Summary struct {
	TotalQueues    int `json:"total_queues"`
	ActiveAlerts   int `json:"active_alerts"`
	CriticalQueues int `json:"critical_queues"`
	SpillingQueues int `json:"spilling_queues"`
}

type RegistrySnapshot struct {
	Summary Summary         `json:"summary"`
	Alerts  []QueueSnapshot `json:"alerts"`
	Queues  []QueueSnapshot `json:"queues"`
}

type Registry struct {
	mu     sync.RWMutex
	cfg    Config
	queues map[string]*QueueHandle
}

func NewRegistry(cfg Config) *Registry {
	return &Registry{cfg: cfg.normalized(), queues: make(map[string]*QueueHandle)}
}

func (r *Registry) Configure(cfg Config) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.cfg = cfg.normalized()
	r.mu.Unlock()
}

func (r *Registry) Config() Config {
	if r == nil {
		return DefaultConfig("")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cfg
}

func (r *Registry) Register(spec QueueSpec) *QueueHandle {
	if r == nil {
		return nil
	}
	spec.Name = strings.TrimSpace(spec.Name)
	if spec.Name == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.queues[spec.Name]; existing != nil {
		existing.mu.Lock()
		existing.category = spec.Category
		existing.criticality = spec.Criticality
		if spec.Capacity > 0 {
			existing.capacity = spec.Capacity
		}
		if spec.LossPolicy != "" {
			existing.lossPolicy = spec.LossPolicy
		}
		if spec.BasisText != "" {
			existing.basisText = spec.BasisText
		}
		existing.updatedAt = time.Now()
		existing.mu.Unlock()
		return existing
	}
	handle := &QueueHandle{
		registry:    r,
		name:        spec.Name,
		category:    strings.TrimSpace(spec.Category),
		criticality: strings.TrimSpace(spec.Criticality),
		capacity:    spec.Capacity,
		lossPolicy:  strings.TrimSpace(spec.LossPolicy),
		basisText:   strings.TrimSpace(spec.BasisText),
		alertLevel:  AlertLevelNormal,
		updatedAt:   time.Now(),
	}
	if handle.capacity <= 0 {
		handle.capacity = 1
	}
	r.queues[spec.Name] = handle
	return handle
}

func (r *Registry) Snapshot() RegistrySnapshot {
	if r == nil {
		return RegistrySnapshot{}
	}
	r.mu.RLock()
	queues := make([]*QueueHandle, 0, len(r.queues))
	for _, q := range r.queues {
		queues = append(queues, q)
	}
	r.mu.RUnlock()

	out := make([]QueueSnapshot, 0, len(queues))
	alerts := make([]QueueSnapshot, 0, len(queues))
	summary := Summary{TotalQueues: len(queues)}
	for _, q := range queues {
		snap := q.Snapshot()
		out = append(out, snap)
		if snap.AlertLevel != AlertLevelNormal {
			summary.ActiveAlerts++
			alerts = append(alerts, snap)
		}
		if snap.AlertLevel == AlertLevelCritical || snap.AlertLevel == AlertLevelEmergency {
			summary.CriticalQueues++
		}
		if snap.SpillActive {
			summary.SpillingQueues++
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	sort.Slice(alerts, func(i, j int) bool {
		if alerts[i].AlertLevel == alerts[j].AlertLevel {
			return alerts[i].Name < alerts[j].Name
		}
		return alertRank(alerts[i].AlertLevel) > alertRank(alerts[j].AlertLevel)
	})
	return RegistrySnapshot{Summary: summary, Alerts: alerts, Queues: out}
}

type QueueHandle struct {
	registry *Registry

	mu sync.Mutex

	name                 string
	category             string
	criticality          string
	capacity             int
	lossPolicy           string
	basisText            string
	currentDepth         int
	highWatermark        int
	enqueueTotal         int64
	dequeueTotal         int64
	dropTotal            int64
	spillTotal           int64
	spillBytes           int64
	spillDepth           int
	alertLevel           string
	lastAlertAt          time.Time
	lastDropAt           time.Time
	lastSpillAt          time.Time
	lastSpillGrowthLogAt time.Time
	updatedAt            time.Time
}

func (h *QueueHandle) Name() string {
	if h == nil {
		return ""
	}
	return h.name
}

func (h *QueueHandle) Capacity() int {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.capacity
}

func (h *QueueHandle) SetCapacity(capacity int) {
	if h == nil {
		return
	}
	if capacity <= 0 {
		capacity = 1
	}
	h.mu.Lock()
	h.capacity = capacity
	h.updateDepthLocked(h.currentDepth)
	h.mu.Unlock()
}

func (h *QueueHandle) SetBasisText(text string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.basisText = strings.TrimSpace(text)
	h.updatedAt = time.Now()
	h.mu.Unlock()
}

func (h *QueueHandle) MarkEnqueued(depth int) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.enqueueTotal++
	h.updateDepthLocked(depth)
	h.mu.Unlock()
}

func (h *QueueHandle) MarkDequeued(depth int) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.dequeueTotal++
	h.updateDepthLocked(depth)
	h.mu.Unlock()
}

func (h *QueueHandle) ObserveDepth(depth int) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.updateDepthLocked(depth)
	h.mu.Unlock()
}

func (h *QueueHandle) MarkDropped(depth int) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.dropTotal++
	h.lastDropAt = time.Now()
	h.updateDepthLocked(depth)
	h.mu.Unlock()
}

func (h *QueueHandle) MarkSpilled(bytes int64, depth int) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.enqueueTotal++
	h.spillTotal++
	h.spillBytes += bytes
	h.spillDepth++
	h.lastSpillAt = time.Now()
	if h.spillDepth == 1 {
		logger.Warn("queue spill started", "queue", h.name, "spill_depth", h.spillDepth)
	} else if h.spillDepth > h.capacity && time.Since(h.lastSpillGrowthLogAt) >= 5*time.Second {
		h.lastSpillGrowthLogAt = time.Now()
		logger.Warn("queue spill backlog growing", "queue", h.name, "spill_depth", h.spillDepth, "capacity", h.capacity)
	}
	h.updateDepthLocked(depth)
	h.mu.Unlock()
}

func (h *QueueHandle) MarkSpillRecovered(depth int) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.spillDepth > 0 {
		h.spillDepth--
	}
	h.updateDepthLocked(depth)
	h.mu.Unlock()
}

func (h *QueueHandle) Snapshot() QueueSnapshot {
	if h == nil {
		return QueueSnapshot{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	usage := 0.0
	if h.capacity > 0 {
		usage = float64(h.currentDepth) * 100 / float64(h.capacity)
	}
	return QueueSnapshot{
		Name:          h.name,
		Category:      h.category,
		Criticality:   h.criticality,
		Capacity:      h.capacity,
		CurrentDepth:  h.currentDepth,
		UsagePercent:  usage,
		HighWatermark: h.highWatermark,
		EnqueueTotal:  h.enqueueTotal,
		DequeueTotal:  h.dequeueTotal,
		DropTotal:     h.dropTotal,
		SpillTotal:    h.spillTotal,
		SpillBytes:    h.spillBytes,
		SpillDepth:    h.spillDepth,
		SpillActive:   h.spillDepth > 0,
		AlertLevel:    h.alertLevel,
		LossPolicy:    h.lossPolicy,
		BasisText:     h.basisText,
		LastAlertAt:   h.lastAlertAt,
		LastDropAt:    h.lastDropAt,
		LastSpillAt:   h.lastSpillAt,
		UpdatedAt:     h.updatedAt,
	}
}

func (h *QueueHandle) updateDepthLocked(depth int) {
	if depth < 0 {
		depth = 0
	}
	h.currentDepth = depth
	if depth > h.highWatermark {
		h.highWatermark = depth
	}
	h.updatedAt = time.Now()
	h.maybeTransitionAlertLocked()
}

func (h *QueueHandle) maybeTransitionAlertLocked() {
	cfg := DefaultConfig("")
	if h.registry != nil {
		cfg = h.registry.Config()
	}
	next := h.alertLevel
	usage := 0.0
	if h.capacity > 0 {
		usage = float64(h.currentDepth) * 100 / float64(h.capacity)
	}
	switch {
	case usage >= float64(cfg.EmergencyPercent):
		next = AlertLevelEmergency
	case usage >= float64(cfg.CriticalPercent):
		next = AlertLevelCritical
	case usage >= float64(cfg.WarnPercent):
		if h.alertLevel == AlertLevelEmergency || h.alertLevel == AlertLevelCritical || h.alertLevel == AlertLevelWarn {
			next = h.alertLevel
		} else {
			next = AlertLevelWarn
		}
	case usage <= float64(cfg.RecoverPercent):
		next = AlertLevelNormal
	default:
		next = h.alertLevel
	}
	if h.alertLevel == AlertLevelNormal && usage >= float64(cfg.WarnPercent) && usage < float64(cfg.CriticalPercent) {
		next = AlertLevelWarn
	}
	if next == h.alertLevel {
		return
	}
	prev := h.alertLevel
	h.alertLevel = next
	h.lastAlertAt = time.Now()
	if next == AlertLevelNormal {
		logger.Info("queue alert recovered", "queue", h.name, "previous_level", prev, "depth", h.currentDepth, "capacity", h.capacity)
		return
	}
	logger.Warn("queue alert level changed", "queue", h.name, "level", next, "depth", h.currentDepth, "capacity", h.capacity, "usage_percent", fmt.Sprintf("%.1f", usage))
}

func alertRank(level string) int {
	switch level {
	case AlertLevelEmergency:
		return 4
	case AlertLevelCritical:
		return 3
	case AlertLevelWarn:
		return 2
	default:
		return 1
	}
}
