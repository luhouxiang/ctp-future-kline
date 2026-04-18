package web

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/appmode"
	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/sessiontime"
)

type chartSessionRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type chartSubscribedPayload struct {
	TicketID       string              `json:"ticket_id"`
	Subscription   map[string]any      `json:"subscription"`
	Sessions       []chartSessionRange `json:"sessions"`
	SessionSource  string              `json:"session_source"`
	SessionVersion string              `json:"session_version"`
	UpdatedAt      string              `json:"updated_at"`
}

type subscriptionTicketEntry struct {
	ticketID       string
	sessions       []sessiontime.Range
	sessionSource  string
	sessionVersion string
	dayKey         string
	mode           string
	updatedAt      time.Time
}

type subscriptionTicketManager struct {
	mu      sync.Mutex
	items   map[string]subscriptionTicketEntry
	db      *sql.DB
	nowFunc func() time.Time
}

func newSubscriptionTicketManager(sharedDSN string) *subscriptionTicketManager {
	dsn := strings.TrimSpace(sharedDSN)
	mgr := &subscriptionTicketManager{
		items:   make(map[string]subscriptionTicketEntry),
		nowFunc: time.Now,
	}
	if dsn == "" {
		return mgr
	}
	db, err := dbx.Open(dsn)
	if err != nil {
		return mgr
	}
	mgr.db = db
	return mgr
}

func (m *subscriptionTicketManager) Close() error {
	if m == nil || m.db == nil {
		return nil
	}
	return m.db.Close()
}

func (m *subscriptionTicketManager) Resolve(owner string, sub map[string]any, variety string, mode string) chartSubscribedPayload {
	now := m.now()
	dayKey := now.Format("20060102")
	sessionRanges, sessionSource, rawVersion := m.loadSessions(variety)
	version := fmt.Sprintf("%s|%s|%s", strings.TrimSpace(rawVersion), dayKey, appmode.Normalize(mode))
	key := ticketCacheKey(owner, sub)

	m.mu.Lock()
	defer m.mu.Unlock()
	if item, ok := m.items[key]; ok {
		if item.sessionVersion == version && item.dayKey == dayKey && item.mode == appmode.Normalize(mode) {
			return chartSubscribedPayload{
				TicketID:       item.ticketID,
				Subscription:   sub,
				Sessions:       encodeChartSessions(item.sessions),
				SessionSource:  item.sessionSource,
				SessionVersion: item.sessionVersion,
				UpdatedAt:      item.updatedAt.Format("2006-01-02 15:04:05"),
			}
		}
	}

	ticketID := fmt.Sprintf("ticket-%d", now.UnixNano())
	entry := subscriptionTicketEntry{
		ticketID:       ticketID,
		sessions:       append([]sessiontime.Range(nil), sessionRanges...),
		sessionSource:  sessionSource,
		sessionVersion: version,
		dayKey:         dayKey,
		mode:           appmode.Normalize(mode),
		updatedAt:      now,
	}
	m.items[key] = entry
	return chartSubscribedPayload{
		TicketID:       ticketID,
		Subscription:   sub,
		Sessions:       encodeChartSessions(entry.sessions),
		SessionSource:  entry.sessionSource,
		SessionVersion: entry.sessionVersion,
		UpdatedAt:      entry.updatedAt.Format("2006-01-02 15:04:05"),
	}
}

func (m *subscriptionTicketManager) now() time.Time {
	if m == nil || m.nowFunc == nil {
		return time.Now()
	}
	return m.nowFunc()
}

func ticketCacheKey(owner string, sub map[string]any) string {
	return strings.Join([]string{
		strings.ToLower(strings.TrimSpace(owner)),
		strings.ToLower(strings.TrimSpace(stringifyAny(sub["symbol"]))),
		strings.ToLower(strings.TrimSpace(stringifyAny(sub["type"]))),
		strings.ToLower(strings.TrimSpace(stringifyAny(sub["variety"]))),
		strings.ToLower(strings.TrimSpace(stringifyAny(sub["data_mode"]))),
	}, "|")
}

func stringifyAny(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

func encodeChartSessions(in []sessiontime.Range) []chartSessionRange {
	out := make([]chartSessionRange, 0, len(in))
	for _, item := range in {
		out = append(out, chartSessionRange{Start: item.Start, End: item.End})
	}
	return out
}

func (m *subscriptionTicketManager) loadSessions(variety string) ([]sessiontime.Range, string, string) {
	key := strings.ToLower(strings.TrimSpace(variety))
	if key == "" {
		def := sessiontime.DefaultRanges()
		return def, "default", "default:empty_variety"
	}
	if m == nil || m.db == nil {
		def := sessiontime.DefaultRanges()
		return def, "default", "default:no_db"
	}
	var raw string
	var completed bool
	err := m.db.QueryRow(`SELECT session_json,is_completed FROM trading_sessions WHERE variety=?`, key).Scan(&raw, &completed)
	if err != nil || !completed {
		def := sessiontime.DefaultRanges()
		if err == nil {
			return def, "default", "default:not_completed"
		}
		return def, "default", "default:no_rows"
	}
	ranges, decErr := sessiontime.DecodeSessionJSON(raw)
	if decErr != nil || len(ranges) == 0 {
		def := sessiontime.DefaultRanges()
		return def, "default", "default:decode_failed"
	}
	return ranges, "db", "db:" + strings.TrimSpace(raw)
}
