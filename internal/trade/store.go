package trade

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	dbx "ctp-go-demo/internal/db"
)

type Store struct {
	db *sql.DB
}

func NewStore(dsn string) (*Store, error) {
	db, err := dbx.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) UpsertTradeAccount(accountID, brokerID, investorID string) error {
	now := time.Now()
	_, err := s.db.Exec(`
INSERT INTO trade_accounts(account_id,broker_id,investor_id,display_name,created_at,updated_at)
VALUES(?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
broker_id=VALUES(broker_id),
investor_id=VALUES(investor_id),
display_name=VALUES(display_name),
updated_at=VALUES(updated_at)
`, accountID, brokerID, investorID, accountID, now, now)
	return err
}

func (s *Store) SaveAccountSnapshot(item TradingAccountSnapshot) error {
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = time.Now()
	}
	_, err := s.db.Exec(`
INSERT INTO trade_account_snapshots(account_id,balance,available,margin_value,frozen_cash,commission,close_profit,position_profit,updated_at)
VALUES(?,?,?,?,?,?,?,?,?)
`, item.AccountID, item.Balance, item.Available, item.Margin, item.FrozenCash, item.Commission, item.CloseProfit, item.PositionProfit, item.UpdatedAt)
	return err
}

func (s *Store) LatestAccountSnapshot(accountID string) (TradingAccountSnapshot, error) {
	var out TradingAccountSnapshot
	err := s.db.QueryRow(`
SELECT account_id,balance,available,margin_value,frozen_cash,commission,close_profit,position_profit,updated_at
FROM trade_account_snapshots
WHERE account_id=?
ORDER BY updated_at DESC,id DESC
LIMIT 1
`, accountID).Scan(&out.AccountID, &out.Balance, &out.Available, &out.Margin, &out.FrozenCash, &out.Commission, &out.CloseProfit, &out.PositionProfit, &out.UpdatedAt)
	return out, err
}

func (s *Store) ReplacePositions(accountID string, items []PositionSnapshot) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.Exec(`DELETE FROM trade_positions WHERE account_id=?`, accountID); err != nil {
		return err
	}
	for _, item := range items {
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = time.Now()
		}
		if _, err = tx.Exec(`
INSERT INTO trade_positions(account_id,symbol,exchange_id,direction,hedge_flag,yd_position,today_position,position,open_cost,position_cost,use_margin,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
`, item.AccountID, item.Symbol, item.Exchange, item.Direction, item.HedgeFlag, item.YdPosition, item.TodayPosition, item.Position, item.OpenCost, item.PositionCost, item.UseMargin, item.UpdatedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListPositions(accountID string) ([]PositionSnapshot, error) {
	rows, err := s.db.Query(`
SELECT account_id,symbol,exchange_id,direction,hedge_flag,yd_position,today_position,position,open_cost,position_cost,use_margin,updated_at
FROM trade_positions
WHERE account_id=?
ORDER BY symbol ASC,direction ASC
`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PositionSnapshot
	for rows.Next() {
		var item PositionSnapshot
		if err := rows.Scan(&item.AccountID, &item.Symbol, &item.Exchange, &item.Direction, &item.HedgeFlag, &item.YdPosition, &item.TodayPosition, &item.Position, &item.OpenCost, &item.PositionCost, &item.UseMargin, &item.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) UpsertOrder(item OrderRecord) error {
	if item.InsertedAt.IsZero() {
		item.InsertedAt = time.Now()
	}
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = item.InsertedAt
	}
	_, err := s.db.Exec(`
INSERT INTO trade_orders(command_id,account_id,order_ref,front_id,session_id,exchange_id,order_sys_id,symbol,direction,offset_flag,limit_price,volume_total_original,volume_traded,volume_canceled,order_status,submit_status,status_msg,inserted_at,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
account_id=VALUES(account_id),
order_ref=VALUES(order_ref),
front_id=VALUES(front_id),
session_id=VALUES(session_id),
exchange_id=VALUES(exchange_id),
order_sys_id=VALUES(order_sys_id),
symbol=VALUES(symbol),
direction=VALUES(direction),
offset_flag=VALUES(offset_flag),
limit_price=VALUES(limit_price),
volume_total_original=VALUES(volume_total_original),
volume_traded=VALUES(volume_traded),
volume_canceled=VALUES(volume_canceled),
order_status=VALUES(order_status),
submit_status=VALUES(submit_status),
status_msg=VALUES(status_msg),
updated_at=VALUES(updated_at)
`, item.CommandID, item.AccountID, item.OrderRef, item.FrontID, item.SessionID, item.ExchangeID, item.OrderSysID, item.Symbol, item.Direction, item.OffsetFlag, item.LimitPrice, item.VolumeTotalOriginal, item.VolumeTraded, item.VolumeCanceled, item.OrderStatus, item.SubmitStatus, item.StatusMsg, item.InsertedAt, item.UpdatedAt)
	return err
}

func (s *Store) GetOrder(commandID string) (OrderRecord, error) {
	var out OrderRecord
	err := s.db.QueryRow(`
SELECT account_id,command_id,order_ref,front_id,session_id,exchange_id,order_sys_id,symbol,direction,offset_flag,limit_price,volume_total_original,volume_traded,volume_canceled,order_status,submit_status,status_msg,inserted_at,updated_at
FROM trade_orders WHERE command_id=?
`, commandID).Scan(&out.AccountID, &out.CommandID, &out.OrderRef, &out.FrontID, &out.SessionID, &out.ExchangeID, &out.OrderSysID, &out.Symbol, &out.Direction, &out.OffsetFlag, &out.LimitPrice, &out.VolumeTotalOriginal, &out.VolumeTraded, &out.VolumeCanceled, &out.OrderStatus, &out.SubmitStatus, &out.StatusMsg, &out.InsertedAt, &out.UpdatedAt)
	return out, err
}

func (s *Store) ListOrders(accountID string, limit int) ([]OrderRecord, error) {
	rows, err := s.db.Query(`
SELECT account_id,command_id,order_ref,front_id,session_id,exchange_id,order_sys_id,symbol,direction,offset_flag,limit_price,volume_total_original,volume_traded,volume_canceled,order_status,submit_status,status_msg,inserted_at,updated_at
FROM trade_orders
WHERE account_id=?
ORDER BY updated_at DESC
LIMIT ?
`, accountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OrderRecord
	for rows.Next() {
		var item OrderRecord
		if err := rows.Scan(&item.AccountID, &item.CommandID, &item.OrderRef, &item.FrontID, &item.SessionID, &item.ExchangeID, &item.OrderSysID, &item.Symbol, &item.Direction, &item.OffsetFlag, &item.LimitPrice, &item.VolumeTotalOriginal, &item.VolumeTraded, &item.VolumeCanceled, &item.OrderStatus, &item.SubmitStatus, &item.StatusMsg, &item.InsertedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) AppendTrade(item TradeRecord) error {
	if item.ReceivedAt.IsZero() {
		item.ReceivedAt = time.Now()
	}
	_, err := s.db.Exec(`
INSERT INTO trade_trades(account_id,trade_id,order_ref,order_sys_id,exchange_id,symbol,direction,offset_flag,price,volume,trade_time,trading_day,received_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
order_ref=VALUES(order_ref),
order_sys_id=VALUES(order_sys_id),
symbol=VALUES(symbol),
direction=VALUES(direction),
offset_flag=VALUES(offset_flag),
price=VALUES(price),
volume=VALUES(volume),
trade_time=VALUES(trade_time),
trading_day=VALUES(trading_day),
received_at=VALUES(received_at)
`, item.AccountID, item.TradeID, item.OrderRef, item.OrderSysID, item.ExchangeID, item.Symbol, item.Direction, item.OffsetFlag, item.Price, item.Volume, item.TradeTime, item.TradingDay, item.ReceivedAt)
	return err
}

func (s *Store) ListTrades(accountID string, limit int) ([]TradeRecord, error) {
	rows, err := s.db.Query(`
SELECT account_id,trade_id,order_ref,order_sys_id,exchange_id,symbol,direction,offset_flag,price,volume,trade_time,trading_day,received_at
FROM trade_trades
WHERE account_id=?
ORDER BY trade_time DESC,id DESC
LIMIT ?
`, accountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TradeRecord
	for rows.Next() {
		var item TradeRecord
		if err := rows.Scan(&item.AccountID, &item.TradeID, &item.OrderRef, &item.OrderSysID, &item.ExchangeID, &item.Symbol, &item.Direction, &item.OffsetFlag, &item.Price, &item.Volume, &item.TradeTime, &item.TradingDay, &item.ReceivedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) AppendCommandAudit(item OrderCommandAudit) (int64, error) {
	reqRaw, err := json.Marshal(item.Request)
	if err != nil {
		return 0, err
	}
	respRaw, err := json.Marshal(item.Response)
	if err != nil {
		return 0, err
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now()
	}
	res, err := s.db.Exec(`
INSERT INTO trade_command_audits(account_id,command_id,command_type,symbol,risk_status,risk_reason,request_json,response_json,created_at)
VALUES(?,?,?,?,?,?,?,?,?)
`, item.AccountID, item.CommandID, item.CommandType, item.Symbol, item.RiskStatus, item.RiskReason, string(reqRaw), string(respRaw), item.CreatedAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) ListCommandAudits(accountID string, limit int) ([]OrderCommandAudit, error) {
	rows, err := s.db.Query(`
SELECT id,account_id,command_id,command_type,symbol,risk_status,risk_reason,request_json,response_json,created_at
FROM trade_command_audits
WHERE account_id=?
ORDER BY created_at DESC,id DESC
LIMIT ?
`, accountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OrderCommandAudit
	for rows.Next() {
		var item OrderCommandAudit
		var reqRaw string
		var respRaw string
		if err := rows.Scan(&item.ID, &item.AccountID, &item.CommandID, &item.CommandType, &item.Symbol, &item.RiskStatus, &item.RiskReason, &reqRaw, &respRaw, &item.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(reqRaw), &item.Request)
		_ = json.Unmarshal([]byte(respRaw), &item.Response)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) AppendQueryAudit(item QueryAudit) error {
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now()
	}
	_, err := s.db.Exec(`
INSERT INTO trade_query_audits(account_id,query_type,status,detail,created_at)
VALUES(?,?,?,?,?)
`, item.AccountID, item.QueryType, item.Status, item.Detail, item.CreatedAt)
	return err
}

func (s *Store) SaveSessionState(item SessionState) error {
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = time.Now()
	}
	_, err := s.db.Exec(`
INSERT INTO trade_session_state(account_id,front_id,session_id,next_order_ref,connected,authenticated,logged_in,settlement_confirmed,trading_day,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
front_id=VALUES(front_id),
session_id=VALUES(session_id),
next_order_ref=VALUES(next_order_ref),
connected=VALUES(connected),
authenticated=VALUES(authenticated),
logged_in=VALUES(logged_in),
settlement_confirmed=VALUES(settlement_confirmed),
trading_day=VALUES(trading_day),
updated_at=VALUES(updated_at)
`, item.AccountID, item.FrontID, item.SessionID, item.NextOrderRef, boolToInt(item.Connected), boolToInt(item.Authenticated), boolToInt(item.LoggedIn), boolToInt(item.SettlementConfirmed), item.TradingDay, item.UpdatedAt)
	return err
}

func (s *Store) LoadSessionState(accountID string) (SessionState, error) {
	var out SessionState
	var connected int
	var authenticated int
	var loggedIn int
	var settlementConfirmed int
	err := s.db.QueryRow(`
SELECT account_id,front_id,session_id,next_order_ref,connected,authenticated,logged_in,settlement_confirmed,trading_day,updated_at
FROM trade_session_state WHERE account_id=?
`, accountID).Scan(&out.AccountID, &out.FrontID, &out.SessionID, &out.NextOrderRef, &connected, &authenticated, &loggedIn, &settlementConfirmed, &out.TradingDay, &out.UpdatedAt)
	if err != nil {
		return out, err
	}
	out.Connected = connected == 1
	out.Authenticated = authenticated == 1
	out.LoggedIn = loggedIn == 1
	out.SettlementConfirmed = settlementConfirmed == 1
	return out, nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func mustCommandID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
