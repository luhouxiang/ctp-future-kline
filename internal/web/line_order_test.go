package web

import (
	"testing"

	"ctp-future-kline/internal/chartlayout"
)

func TestDrawingLinePriceHorizontalAndTrendline(t *testing.T) {
	price := 3650.0
	hline := chartlayout.DrawingObject{
		Type:   "hline",
		Points: []chartlayout.DrawingPoint{{Time: 100, Price: &price}},
	}
	got, ok := drawingLinePrice(hline, 120)
	if !ok || got != 3650 {
		t.Fatalf("hline price = %v,%v want 3650,true", got, ok)
	}

	p1 := 100.0
	p2 := 120.0
	trendline := chartlayout.DrawingObject{
		Type: "trendline",
		Points: []chartlayout.DrawingPoint{
			{Time: 100, Price: &p1},
			{Time: 200, Price: &p2},
		},
	}
	got, ok = drawingLinePrice(trendline, 150)
	if !ok || got != 110 {
		t.Fatalf("trendline price = %v,%v want 110,true", got, ok)
	}
}

func TestShouldTriggerCrossAndTouch(t *testing.T) {
	if !shouldTrigger("cross_up", 99, 101, 100, 100, 1) {
		t.Fatal("cross_up should trigger")
	}
	if shouldTrigger("cross_up", 101, 99, 100, 100, 1) {
		t.Fatal("cross_up should not trigger on down cross")
	}
	if !shouldTrigger("cross_down", 101, 99, 100, 100, 1) {
		t.Fatal("cross_down should trigger")
	}
	if !shouldTrigger("touch", 99, 100.4, 100, 100, 1) {
		t.Fatal("touch should trigger within half tick")
	}
	if shouldTrigger("touch", 99, 99.4, 100, 100, 1) {
		t.Fatal("touch should not trigger before crossing outside half tick")
	}
}
