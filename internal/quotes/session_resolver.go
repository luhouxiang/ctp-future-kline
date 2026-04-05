package quotes

import (
	"database/sql"
	"fmt"
	"sync"

	dbx "ctp-future-kline/internal/db"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/sessiontime"
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

func preferMetaDB(metaDB *sql.DB, fallback *sql.DB) *sql.DB {
	if metaDB != nil {
		return metaDB
	}
	return fallback
}

func (r *sessionResolver) Sessions(variety string) ([]sessiontime.Range, error) {
	variety = normalizeVariety(variety)
	if variety == "" {
		return nil, nil
	}
	if r == nil || r.db == nil {
		logger.Debug("trading sessions resolved", "variety", variety, "database", "<nil>", "source", "default", "reason", "no_db", "session_text", sessiontime.DefaultSessionText)
		return sessiontime.DefaultRanges(), nil
	}
	r.mu.Lock()
	if cached, ok := r.cache[variety]; ok {
		r.mu.Unlock()
		return append([]sessiontime.Range(nil), cached...), nil
	}
	r.mu.Unlock()

	var raw string
	var completed bool
	dbName, dbErr := dbx.CurrentDatabase(r.db)
	if dbErr != nil {
		dbName = "<unknown>"
	}
	err := r.db.QueryRow(`SELECT session_json,is_completed FROM trading_sessions WHERE variety=?`, variety).Scan(&raw, &completed)
	if err != nil {
		if err == sql.ErrNoRows {
			ranges := sessiontime.DefaultRanges()
			r.mu.Lock()
			r.cache[variety] = append([]sessiontime.Range(nil), ranges...)
			r.mu.Unlock()
			logger.Debug("trading sessions resolved", "variety", variety, "database", dbName, "source", "default", "reason", "no_rows", "session_text", sessiontime.DefaultSessionText)
			return append([]sessiontime.Range(nil), ranges...), nil
		}
		return nil, fmt.Errorf("query trading_sessions failed: %w", err)
	}
	if !completed {
		ranges := sessiontime.DefaultRanges()
		r.mu.Lock()
		r.cache[variety] = append([]sessiontime.Range(nil), ranges...)
		r.mu.Unlock()
		logger.Debug("trading sessions resolved", "variety", variety, "database", dbName, "source", "default", "reason", "not_completed", "session_text", sessiontime.DefaultSessionText)
		return append([]sessiontime.Range(nil), ranges...), nil
	}
	ranges, err := sessiontime.DecodeSessionJSON(raw)
	if err != nil {
		return nil, err
	}
	logger.Debug("trading sessions resolved", "variety", variety, "database", dbName, "source", "db", "session_text", raw, "is_completed", completed)
	r.mu.Lock()
	r.cache[variety] = append([]sessiontime.Range(nil), ranges...)
	r.mu.Unlock()
	return append([]sessiontime.Range(nil), ranges...), nil
}
