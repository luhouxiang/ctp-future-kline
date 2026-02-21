package chartlayout

import (
	"fmt"
	"strings"
)

type Service struct {
	store *Store
}

func NewService(store *Store) *Service {
	return &Service{store: store}
}

func (s *Service) Get(owner, symbol, kind, variety, timeframe string) (LayoutSnapshot, bool, error) {
	scope, err := normalizeScope(owner, symbol, kind, variety, timeframe)
	if err != nil {
		return LayoutSnapshot{}, false, err
	}
	return s.store.GetLayout(scope)
}

func (s *Service) Put(snapshot LayoutSnapshot) (LayoutSnapshot, error) {
	scope, err := normalizeScope(snapshot.Owner, snapshot.Symbol, snapshot.Type, snapshot.Variety, snapshot.Timeframe)
	if err != nil {
		return LayoutSnapshot{}, err
	}
	snapshot.Owner = scope.Owner
	snapshot.Symbol = scope.Symbol
	snapshot.Type = scope.Kind
	snapshot.Variety = scope.Variety
	snapshot.Timeframe = scope.Timeframe
	if strings.TrimSpace(snapshot.Theme) == "" {
		snapshot.Theme = "dark"
	}
	drawings, err := normalizeDrawings(snapshot.Drawings, scope)
	if err != nil {
		return LayoutSnapshot{}, err
	}
	snapshot.Drawings = drawings
	if err := s.store.PutLayout(snapshot); err != nil {
		return LayoutSnapshot{}, err
	}
	out, _, err := s.store.GetLayout(scope)
	if err != nil {
		return LayoutSnapshot{}, err
	}
	return out, nil
}

func (s *Service) UpsertDrawing(d DrawingObject) (DrawingObject, error) {
	scope, err := normalizeScope(d.Owner, d.Symbol, d.Kind, d.Variety, d.Timeframe)
	if err != nil {
		return DrawingObject{}, err
	}
	if strings.TrimSpace(d.Type) == "" {
		return DrawingObject{}, fmt.Errorf("drawing type is required")
	}
	d = normalizeDrawing(d, scope)
	if err := validateDrawing(d); err != nil {
		return DrawingObject{}, err
	}
	return s.store.UpsertDrawing(scope, d)
}

func (s *Service) DeleteDrawing(owner, symbol, kind, variety, timeframe, id string) (bool, error) {
	scope, err := normalizeScope(owner, symbol, kind, variety, timeframe)
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(id) == "" {
		return false, fmt.Errorf("id is required")
	}
	return s.store.DeleteDrawing(scope, id)
}

func normalizeScope(owner, symbol, kind, variety, timeframe string) (Scope, error) {
	scope := Scope{
		Owner:     strings.TrimSpace(owner),
		Symbol:    strings.TrimSpace(symbol),
		Kind:      strings.ToLower(strings.TrimSpace(kind)),
		Variety:   strings.ToLower(strings.TrimSpace(variety)),
		Timeframe: strings.ToLower(strings.TrimSpace(timeframe)),
	}
	if scope.Owner == "" {
		scope.Owner = "admin"
	}
	if scope.Symbol == "" {
		return Scope{}, fmt.Errorf("symbol is required")
	}
	if scope.Kind != "contract" && scope.Kind != "l9" {
		return Scope{}, fmt.Errorf("type must be contract or l9")
	}
	if scope.Timeframe == "" {
		scope.Timeframe = "1m"
	}
	if scope.Variety == "" {
		scope.Variety = inferVariety(scope.Symbol)
	}
	return scope, nil
}

func normalizeDrawings(in []DrawingObject, scope Scope) ([]DrawingObject, error) {
	out := make([]DrawingObject, 0, len(in))
	for i := range in {
		d := normalizeDrawing(in[i], scope)
		if err := validateDrawing(d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

func normalizeDrawing(d DrawingObject, scope Scope) DrawingObject {
	d.Owner = scope.Owner
	d.Symbol = scope.Symbol
	d.Kind = scope.Kind
	d.Variety = scope.Variety
	d.Timeframe = scope.Timeframe
	if strings.TrimSpace(d.ObjectClass) == "" {
		if d.Type == "trendline" {
			d.ObjectClass = "trendline"
		} else {
			d.ObjectClass = "general"
		}
	}
	if strings.TrimSpace(d.VisibleRange) == "" {
		d.VisibleRange = "all"
	}
	if d.LineColor == "" {
		d.LineColor = d.Style.Color
	}
	if d.LineWidth == nil {
		d.LineWidth = d.Style.Width
	}
	if d.LineStyle == "" {
		d.LineStyle = "solid"
	}
	if d.LeftCap == "" {
		d.LeftCap = "plain"
	}
	if d.RightCap == "" {
		d.RightCap = "plain"
	}
	if d.LabelText == "" && d.Text != "" {
		d.LabelText = d.Text
	}
	if d.LabelPos == "" {
		d.LabelPos = "middle"
	}
	if d.LabelAlign == "" {
		d.LabelAlign = "center"
	}
	if d.Type == "trendline" && len(d.Points) >= 2 {
		if d.StartTime == 0 {
			d.StartTime = d.Points[0].Time
		}
		if d.EndTime == 0 {
			d.EndTime = d.Points[1].Time
		}
		if d.StartPrice == nil {
			d.StartPrice = d.Points[0].Price
		}
		if d.EndPrice == nil {
			d.EndPrice = d.Points[1].Price
		}
	}
	return d
}

func validateDrawing(d DrawingObject) error {
	if !isOneOf(d.LineStyle, "solid", "dashed", "dotted") {
		return fmt.Errorf("line_style must be solid|dashed|dotted")
	}
	if !isOneOf(d.LeftCap, "plain", "arrow") {
		return fmt.Errorf("left_cap must be plain|arrow")
	}
	if !isOneOf(d.RightCap, "plain", "arrow") {
		return fmt.Errorf("right_cap must be plain|arrow")
	}
	if !isOneOf(d.LabelPos, "top", "middle", "bottom") {
		return fmt.Errorf("label_pos must be top|middle|bottom")
	}
	if !isOneOf(d.LabelAlign, "left", "center", "right") {
		return fmt.Errorf("label_align must be left|center|right")
	}
	if !isOneOf(d.VisibleRange, "all", "minute", "hour", "day") {
		return fmt.Errorf("visible_range must be all|minute|hour|day")
	}
	return nil
}

func isOneOf(v string, vals ...string) bool {
	for _, it := range vals {
		if v == it {
			return true
		}
	}
	return false
}

func inferVariety(symbol string) string {
	s := strings.ToLower(strings.TrimSpace(symbol))
	end := 0
	for _, ch := range s {
		if ch >= 'a' && ch <= 'z' {
			end++
			continue
		}
		break
	}
	if end > 0 {
		return s[:end]
	}
	return ""
}
