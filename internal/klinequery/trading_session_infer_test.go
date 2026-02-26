package klinequery

import "testing"

func TestPickRecent5AndMiddle3(t *testing.T) {
	days := []string{
		"2026-01-01",
		"2026-01-02",
		"2026-01-03",
		"2026-01-04",
		"2026-01-05",
		"2026-01-06",
	}
	recent5 := pickRecent5TradingDays(days)
	if len(recent5) != 5 {
		t.Fatalf("recent5 len=%d, want 5", len(recent5))
	}
	middle3 := pickMiddle3OfRecent5(recent5)
	want := []string{"2026-01-03", "2026-01-04", "2026-01-05"}
	if len(middle3) != len(want) {
		t.Fatalf("middle3 len=%d, want %d", len(middle3), len(want))
	}
	for i := range want {
		if middle3[i] != want[i] {
			t.Fatalf("middle3[%d]=%s, want %s", i, middle3[i], want[i])
		}
	}
}

func TestMinuteSetEqual(t *testing.T) {
	a := map[int]struct{}{540: {}, 545: {}, 550: {}}
	b := map[int]struct{}{550: {}, 540: {}, 545: {}}
	c := map[int]struct{}{540: {}, 550: {}}
	if !minuteSetEqual(a, b) {
		t.Fatalf("a and b should be equal")
	}
	if minuteSetEqual(a, c) {
		t.Fatalf("a and c should not be equal")
	}
}

func TestFloorMinuteTo5(t *testing.T) {
	cases := map[int]int{
		0:    0,
		1:    0,
		4:    0,
		5:    5,
		901:  900, // 15:01 -> 15:00
		1439: 1435,
	}
	for in, want := range cases {
		got := floorMinuteTo5(in)
		if got != want {
			t.Fatalf("floorMinuteTo5(%d)=%d, want %d", in, got, want)
		}
	}
}
