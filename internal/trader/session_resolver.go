package trader

import (
	"database/sql"
	"fmt"
	"sync"

	"ctp-go-demo/internal/sessiontime"
)

type sessionResolver struct {
	db *sql.DB

	mu    sync.Mutex
	cache map[string][]sessiontime.Range
}

func newSessionResolver(db *sql.DB) *sessionResolver {
	if db == nil {
		return nil
	}
	return &sessionResolver{
		db:    db,
		cache: make(map[string][]sessiontime.Range),
	}
}

func (r *sessionResolver) Sessions(variety string) ([]sessiontime.Range, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	variety = normalizeVariety(variety)
	if variety == "" {
		return nil, nil
	}
	r.mu.Lock()
	if cached, ok := r.cache[variety]; ok {
		r.mu.Unlock()
		return append([]sessiontime.Range(nil), cached...), nil
	}
	r.mu.Unlock()

	var raw string
	var completed bool
	err := r.db.QueryRow(`SELECT session_json,is_completed FROM trading_sessions WHERE variety=?`, variety).Scan(&raw, &completed)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query trading_sessions failed: %w", err)
	}
	if !completed {
		return nil, nil
	}
	ranges, err := sessiontime.DecodeSessionJSON(raw)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.cache[variety] = append([]sessiontime.Range(nil), ranges...)
	r.mu.Unlock()
	return append([]sessiontime.Range(nil), ranges...), nil
}
