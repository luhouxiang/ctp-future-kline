package chartlayout

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"ctp-go-demo/internal/logger"
)

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("nil db")
	}
	return &Store{db: db}, nil
}

func (s *Store) GetLayout(scope Scope) (LayoutSnapshot, bool, error) {
	var snapshot LayoutSnapshot
	row := s.db.QueryRow(`
SELECT layout_json, updated_at
FROM chart_layouts
WHERE owner=? AND symbol=? AND kind=? AND variety=? AND timeframe=?`,
		scope.Owner, scope.Symbol, scope.Kind, scope.Variety, scope.Timeframe,
	)
	var raw []byte
	var updatedAt time.Time
	if err := row.Scan(&raw, &updatedAt); err != nil {
		if err == sql.ErrNoRows {
			return LayoutSnapshot{}, false, nil
		}
		return LayoutSnapshot{}, false, err
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			return LayoutSnapshot{}, false, fmt.Errorf("unmarshal layout_json failed: %w", err)
		}
	}
	snapshot.Symbol = scope.Symbol
	snapshot.Type = scope.Kind
	snapshot.Variety = scope.Variety
	snapshot.Timeframe = scope.Timeframe
	snapshot.Owner = scope.Owner
	snapshot.UpdatedAt = updatedAt
	drawings, err := s.ListDrawings(scope)
	if err != nil {
		return LayoutSnapshot{}, false, err
	}
	snapshot.Drawings = drawings
	return snapshot, true, nil
}

func (s *Store) PutLayout(snapshot LayoutSnapshot) error {
	now := time.Now()
	snapshot.UpdatedAt = now
	layoutOnly := snapshot
	layoutOnly.Drawings = nil
	raw, err := json.Marshal(layoutOnly)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	_, err = tx.Exec(`
INSERT INTO chart_layouts(owner,symbol,kind,variety,timeframe,theme,layout_json,updated_at)
VALUES(?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
  theme=VALUES(theme),
  layout_json=VALUES(layout_json),
  updated_at=VALUES(updated_at)`,
		snapshot.Owner,
		snapshot.Symbol,
		snapshot.Type,
		snapshot.Variety,
		snapshot.Timeframe,
		snapshot.Theme,
		string(raw),
		now,
	)
	if err != nil {
		return fmt.Errorf("upsert chart_layouts failed: %w", err)
	}
	if _, err = tx.Exec(`DELETE FROM chart_drawings WHERE owner=? AND symbol=? AND kind=? AND variety=? AND timeframe=?`, snapshot.Owner, snapshot.Symbol, snapshot.Type, snapshot.Variety, snapshot.Timeframe); err != nil {
		return fmt.Errorf("delete chart_drawings failed: %w", err)
	}
	if err = insertDrawingsTx(tx, snapshot.Drawings, snapshot.Owner, snapshot.Symbol, snapshot.Type, snapshot.Variety, snapshot.Timeframe, now); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	trendlineCount := 0
	for i := range snapshot.Drawings {
		if snapshot.Drawings[i].Type == "trendline" {
			trendlineCount++
		}
	}
	logger.Info("chart layout persist success",
		"owner", snapshot.Owner,
		"symbol", snapshot.Symbol,
		"type_kind", snapshot.Type,
		"variety", snapshot.Variety,
		"timeframe", snapshot.Timeframe,
		"drawing_count", len(snapshot.Drawings),
		"trendline_count", trendlineCount,
	)
	if len(snapshot.Drawings) > 0 && trendlineCount == 0 {
		logger.Info("chart layout persist note", "message", "drawings persisted but none is trendline; trendline sql log will not appear")
	}
	return nil
}

func (s *Store) ListDrawings(scope Scope) ([]DrawingObject, error) {
	rows, err := s.db.Query(`
SELECT id,type,points_json,text_value,style_json,locked,visible,z_index,created_at,updated_at,
owner,object_class,start_time,end_time,start_price,end_price,line_color,line_width,line_style,left_cap,right_cap,label_text,label_pos,label_align,visible_range
FROM chart_drawings
WHERE owner=? AND symbol=? AND kind=? AND variety=? AND timeframe=?
ORDER BY z_index ASC, updated_at ASC`, scope.Owner, scope.Symbol, scope.Kind, scope.Variety, scope.Timeframe)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]DrawingObject, 0, 32)
	for rows.Next() {
		var obj DrawingObject
		var pointsRaw []byte
		var styleRaw []byte
		var locked, visible int
		var startTime, endTime sql.NullInt64
		var startPrice, endPrice, lineWidth sql.NullFloat64
		var owner, objectClass, lineColor, lineStyle, leftCap, rightCap, labelText, labelPos, labelAlign, visibleRange sql.NullString
		if err := rows.Scan(
			&obj.ID, &obj.Type, &pointsRaw, &obj.Text, &styleRaw, &locked, &visible, &obj.Z, &obj.CreatedAt, &obj.UpdatedAt,
			&owner, &objectClass, &startTime, &endTime, &startPrice, &endPrice, &lineColor, &lineWidth, &lineStyle, &leftCap, &rightCap, &labelText, &labelPos, &labelAlign, &visibleRange,
		); err != nil {
			return nil, err
		}
		obj.Owner = owner.String
		obj.ObjectClass = objectClass.String
		if startTime.Valid {
			obj.StartTime = startTime.Int64
		}
		if endTime.Valid {
			obj.EndTime = endTime.Int64
		}
		if startPrice.Valid {
			v := startPrice.Float64
			obj.StartPrice = &v
		}
		if endPrice.Valid {
			v := endPrice.Float64
			obj.EndPrice = &v
		}
		obj.LineColor = lineColor.String
		if lineWidth.Valid {
			v := lineWidth.Float64
			obj.LineWidth = &v
		}
		obj.LineStyle = lineStyle.String
		obj.LeftCap = leftCap.String
		obj.RightCap = rightCap.String
		obj.LabelText = labelText.String
		obj.LabelPos = labelPos.String
		obj.LabelAlign = labelAlign.String
		obj.VisibleRange = visibleRange.String
		obj.Owner = scope.Owner
		obj.Symbol = scope.Symbol
		obj.Kind = scope.Kind
		obj.Variety = scope.Variety
		obj.Timeframe = scope.Timeframe
		obj.Locked = locked != 0
		obj.Visible = visible != 0
		if len(pointsRaw) > 0 {
			if err := json.Unmarshal(pointsRaw, &obj.Points); err != nil {
				return nil, fmt.Errorf("unmarshal points_json failed: %w", err)
			}
		}
		if len(styleRaw) > 0 {
			if err := json.Unmarshal(styleRaw, &obj.Style); err != nil {
				return nil, fmt.Errorf("unmarshal style_json failed: %w", err)
			}
		}
		out = append(out, obj)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func insertDrawingsTx(tx *sql.Tx, drawings []DrawingObject, owner, symbol, kind, variety, timeframe string, now time.Time) error {
	for i := range drawings {
		d := drawings[i]
		if strings.TrimSpace(d.ID) == "" {
			d.ID = fmt.Sprintf("drw_%d_%d", now.UnixNano(), i)
		}
		pointsRaw, err := json.Marshal(d.Points)
		if err != nil {
			return err
		}
		styleRaw, err := json.Marshal(d.Style)
		if err != nil {
			return err
		}
		createdAt := d.CreatedAt
		if createdAt.IsZero() {
			createdAt = now
		}
		updatedAt := d.UpdatedAt
		if updatedAt.IsZero() {
			updatedAt = now
		}
		locked := 0
		if d.Locked {
			locked = 1
		}
		visible := 0
		if d.Visible {
			visible = 1
		}
		insertSQL := `
INSERT INTO chart_drawings(
  id,owner,symbol,kind,variety,timeframe,type,
  points_json,text_value,style_json,
  object_class,start_time,end_time,start_price,end_price,
  line_color,line_width,line_style,left_cap,right_cap,
  label_text,label_pos,label_align,visible_range,
  locked,visible,z_index,created_at,updated_at
)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
		args := []any{
			d.ID,
			owner,
			symbol,
			kind,
			variety,
			timeframe,
			d.Type,
			string(pointsRaw),
			d.Text,
			string(styleRaw),
			d.ObjectClass,
			d.StartTime,
			d.EndTime,
			d.StartPrice,
			d.EndPrice,
			d.LineColor,
			d.LineWidth,
			d.LineStyle,
			d.LeftCap,
			d.RightCap,
			d.LabelText,
			d.LabelPos,
			d.LabelAlign,
			d.VisibleRange,
			locked,
			visible,
			d.Z,
			createdAt,
			updatedAt,
		}
		if d.Type == "trendline" {
			logger.Info("trendline save sql",
				"entry", "layout_put",
				"id", d.ID,
				"owner", owner,
				"symbol", symbol,
				"type_kind", kind,
				"timeframe", timeframe,
				"sql", renderExecutableSQL(insertSQL, args...),
			)
		}
		if _, err := tx.Exec(insertSQL, args...); err != nil {
			return fmt.Errorf("insert drawing failed: %w", err)
		}
		if d.Type == "trendline" {
			logger.Info("trendline save success",
				"entry", "layout_put",
				"id", d.ID,
				"owner", owner,
				"symbol", symbol,
				"type_kind", kind,
				"timeframe", timeframe,
			)
		}
	}
	return nil
}

func (s *Store) UpsertDrawing(scope Scope, d DrawingObject) (DrawingObject, error) {
	now := time.Now()
	if strings.TrimSpace(d.ID) == "" {
		d.ID = fmt.Sprintf("drw_%d", now.UnixNano())
	}
	pointsRaw, err := json.Marshal(d.Points)
	if err != nil {
		return DrawingObject{}, err
	}
	styleRaw, err := json.Marshal(d.Style)
	if err != nil {
		return DrawingObject{}, err
	}
	createdAt := d.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := now
	locked := 0
	if d.Locked {
		locked = 1
	}
	visible := 0
	if d.Visible {
		visible = 1
	}
	upsertSQL := `
INSERT INTO chart_drawings(
  id,owner,symbol,kind,variety,timeframe,type,
  points_json,text_value,style_json,
  object_class,start_time,end_time,start_price,end_price,
  line_color,line_width,line_style,left_cap,right_cap,
  label_text,label_pos,label_align,visible_range,
  locked,visible,z_index,created_at,updated_at
)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
  type=VALUES(type),
  points_json=VALUES(points_json),
  text_value=VALUES(text_value),
  style_json=VALUES(style_json),
  object_class=VALUES(object_class),
  start_time=VALUES(start_time),
  end_time=VALUES(end_time),
  start_price=VALUES(start_price),
  end_price=VALUES(end_price),
  line_color=VALUES(line_color),
  line_width=VALUES(line_width),
  line_style=VALUES(line_style),
  left_cap=VALUES(left_cap),
  right_cap=VALUES(right_cap),
  label_text=VALUES(label_text),
  label_pos=VALUES(label_pos),
  label_align=VALUES(label_align),
  visible_range=VALUES(visible_range),
  locked=VALUES(locked),
  visible=VALUES(visible),
  z_index=VALUES(z_index),
  updated_at=VALUES(updated_at)`
	args := []any{
		d.ID, scope.Owner, scope.Symbol, scope.Kind, scope.Variety, scope.Timeframe, d.Type, string(pointsRaw), d.Text, string(styleRaw), d.ObjectClass, d.StartTime, d.EndTime, d.StartPrice, d.EndPrice, d.LineColor, d.LineWidth, d.LineStyle, d.LeftCap, d.RightCap, d.LabelText, d.LabelPos, d.LabelAlign, d.VisibleRange, locked, visible, d.Z, createdAt, updatedAt,
	}
	if d.Type == "trendline" {
		logger.Info("trendline save sql",
			"entry", "upsert_drawing",
			"id", d.ID,
			"owner", scope.Owner,
			"symbol", scope.Symbol,
			"type_kind", scope.Kind,
			"timeframe", scope.Timeframe,
			"sql", renderExecutableSQL(upsertSQL, args...),
		)
	}
	if _, err := s.db.Exec(upsertSQL, args...); err != nil {
		return DrawingObject{}, err
	}
	if d.Type == "trendline" {
		logger.Info("trendline save success",
			"entry", "upsert_drawing",
			"id", d.ID,
			"owner", scope.Owner,
			"symbol", scope.Symbol,
			"type_kind", scope.Kind,
			"timeframe", scope.Timeframe,
		)
	}
	d.Owner = scope.Owner
	d.Symbol = scope.Symbol
	d.Kind = scope.Kind
	d.Variety = scope.Variety
	d.Timeframe = scope.Timeframe
	d.CreatedAt = createdAt
	d.UpdatedAt = updatedAt
	return d, nil
}

func (s *Store) DeleteDrawing(scope Scope, id string) (bool, error) {
	res, err := s.db.Exec(`DELETE FROM chart_drawings WHERE id=? AND owner=? AND symbol=? AND kind=? AND variety=? AND timeframe=?`, id, scope.Owner, scope.Symbol, scope.Kind, scope.Variety, scope.Timeframe)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func renderExecutableSQL(query string, args ...any) string {
	out := strings.TrimSpace(query)
	for _, arg := range args {
		out = strings.Replace(out, "?", mysqlLiteral(arg), 1)
	}
	return navicatSQL(out)
}

func navicatSQL(query string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(query)), " ")
}

func mysqlLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return "NULL"
		}
		return mysqlLiteral(rv.Elem().Interface())
	}
	switch x := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(x), "'", "''") + "'"
	case bool:
		if x {
			return "1"
		}
		return "0"
	case int:
		return strconv.Itoa(x)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case time.Time:
		return "'" + x.Format("2006-01-02 15:04:05") + "'"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(v), "'", "''") + "'"
	}
}
