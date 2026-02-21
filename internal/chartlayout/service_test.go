package chartlayout

import "testing"

func TestNormalizeScopeDefaultOwner(t *testing.T) {
	scope, err := normalizeScope("", "2605", "contract", "sr", "1m")
	if err != nil {
		t.Fatalf("normalizeScope() error = %v", err)
	}
	if scope.Owner != "admin" {
		t.Fatalf("scope.Owner = %q, want admin", scope.Owner)
	}
}

func TestValidateDrawingRejectsInvalidEnums(t *testing.T) {
	d := DrawingObject{
		Type:         "trendline",
		LineStyle:    "invalid",
		LeftCap:      "plain",
		RightCap:     "plain",
		LabelPos:     "middle",
		LabelAlign:   "center",
		VisibleRange: "all",
	}
	if err := validateDrawing(d); err == nil {
		t.Fatalf("validateDrawing() error = nil, want non-nil")
	}
}

func TestNormalizeDrawingFillsTrendlineFields(t *testing.T) {
	p1 := 5234.1
	p2 := 5240.3
	d := DrawingObject{
		Type: "trendline",
		Points: []DrawingPoint{
			{Time: 1700000000, Price: &p1},
			{Time: 1700000600, Price: &p2},
		},
		Style: DrawingStyle{Color: "#4ea1ff", Width: floatPtr(2)},
		Text:  "test",
	}
	scope := Scope{Owner: "admin", Symbol: "2605", Kind: "contract", Variety: "sr", Timeframe: "1m"}
	out := normalizeDrawing(d, scope)
	if out.Owner != "admin" {
		t.Fatalf("out.Owner = %q, want admin", out.Owner)
	}
	if out.StartTime != 1700000000 || out.EndTime != 1700000600 {
		t.Fatalf("time range = (%d,%d), want (1700000000,1700000600)", out.StartTime, out.EndTime)
	}
	if out.StartPrice == nil || out.EndPrice == nil {
		t.Fatalf("price range should not be nil")
	}
	if out.ObjectClass != "trendline" {
		t.Fatalf("out.ObjectClass = %q, want trendline", out.ObjectClass)
	}
	if out.VisibleRange != "all" {
		t.Fatalf("out.VisibleRange = %q, want all", out.VisibleRange)
	}
	if out.LabelText != "test" {
		t.Fatalf("out.LabelText = %q, want test", out.LabelText)
	}
}

func floatPtr(v float64) *float64 {
	return &v
}
