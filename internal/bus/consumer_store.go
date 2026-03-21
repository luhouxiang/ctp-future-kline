package bus

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type ConsumerStore struct {
	// db 是去重存储使用的数据库连接。
	db *sql.DB
}

func NewConsumerStore(db *sql.DB) (*ConsumerStore, error) {
	if db == nil {
		return nil, fmt.Errorf("nil db")
	}
	s := &ConsumerStore{db: db}
	if err := s.ensureTable(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *ConsumerStore) MarkIfFirst(consumerID string, eventID string) (bool, error) {
	if consumerID == "" || eventID == "" {
		return false, fmt.Errorf("consumer_id and event_id required")
	}
	res, err := s.db.Exec(
		`INSERT INTO bus_consume_dedup(consumer_id,event_id,processed_at) VALUES(?,?,?)`,
		consumerID,
		eventID,
		time.Now(),
	)
	if err != nil {
		if isDuplicateInsertError(err) {
			return false, nil
		}
		return false, fmt.Errorf("insert dedup record failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected failed: %w", err)
	}
	return n > 0, nil
}

// ClearAll 会清空 replay consumer 的去重记账表。
// 这通常用于“从头完整回放”的场景：删除所有 (consumer_id,event_id) 记账后，
// 后续 replay 会把历史事件重新视为首次消费。
func (s *ConsumerStore) ClearAll() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("nil consumer store")
	}
	if _, err := s.db.Exec(`DELETE FROM bus_consume_dedup`); err != nil {
		return fmt.Errorf("clear bus_consume_dedup failed: %w", err)
	}
	return nil
}

func (s *ConsumerStore) ensureTable() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS bus_consume_dedup (
  consumer_id VARCHAR(128) NOT NULL,
  event_id VARCHAR(128) NOT NULL,
  processed_at DATETIME NOT NULL,
  PRIMARY KEY (consumer_id, event_id)
)`)
	if err != nil {
		return fmt.Errorf("create bus_consume_dedup failed: %w", err)
	}
	_, err = s.db.Exec(`CREATE INDEX idx_bus_consume_dedup_processed_at ON bus_consume_dedup(processed_at DESC)`)
	if err != nil && !isDuplicateIndexError(err) {
		return fmt.Errorf("create dedup index failed: %w", err)
	}
	return nil
}

func isDuplicateIndexError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "already exists")
}

func isDuplicateInsertError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate entry") ||
		strings.Contains(msg, "unique constraint failed") ||
		strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "constraint failed")
}
