package sessiontime

import "testing"

func TestLabelMinuteNightSession(t *testing.T) {
	t.Parallel()

	sessions, err := ParseSessionText("21:00-02:30")
	if err != nil {
		t.Fatalf("ParseSessionText() error = %v", err)
	}

	cases := []struct {
		raw  int
		want int
	}{
		{20*60 + 59, 21*60 + 1},
		{21 * 60, 21*60 + 1},
		{21*60 + 13, 21*60 + 14},
		{2*60 + 29, 2*60 + 30},
		{2*60 + 30, 2*60 + 30},
	}
	for _, tc := range cases {
		got, ok := LabelMinute(tc.raw, sessions)
		if !ok {
			t.Fatalf("LabelMinute(%d) not mapped", tc.raw)
		}
		if got != tc.want {
			t.Fatalf("LabelMinute(%d) = %d, want %d", tc.raw, got, tc.want)
		}
	}
}

func TestBuildLabelMinuteIndexDaySession(t *testing.T) {
	t.Parallel()

	sessions, err := ParseSessionText("09:00-10:15")
	if err != nil {
		t.Fatalf("ParseSessionText() error = %v", err)
	}
	idx := BuildLabelMinuteIndex(sessions)
	if _, ok := idx[9*60]; ok {
		t.Fatal("09:00 should not appear in label-minute index")
	}
	if _, ok := idx[9*60+1]; !ok {
		t.Fatal("09:01 should appear in label-minute index")
	}
	if _, ok := idx[10*60+15]; !ok {
		t.Fatal("10:15 should appear in label-minute index")
	}
}

func TestDistanceToTradingWindow(t *testing.T) {
	t.Parallel()

	sessions, err := ParseSessionText("21:00-02:30")
	if err != nil {
		t.Fatalf("ParseSessionText() error = %v", err)
	}

	cases := []struct {
		name string
		raw  int
		want int
	}{
		{name: "in session", raw: 21 * 60, want: 0},
		{name: "one minute before night open", raw: 20*60 + 58, want: 1},
		{name: "three minutes after close", raw: 2*60 + 33, want: 3},
		{name: "four minutes after close", raw: 2*60 + 34, want: 4},
		{name: "far from all sessions", raw: 18*60 + 38, want: 141},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := DistanceToTradingWindow(tc.raw, sessions); got != tc.want {
				t.Fatalf("DistanceToTradingWindow(%d) = %d, want %d", tc.raw, got, tc.want)
			}
		})
	}
}

func TestDefaultRanges(t *testing.T) {
	t.Parallel()

	ranges := DefaultRanges()
	if len(ranges) != 5 {
		t.Fatalf("DefaultRanges() len=%d want 5", len(ranges))
	}
	if ranges[0].Start != 21*60 || ranges[0].End != 23*60+59 {
		t.Fatalf("night first segment = %+v", ranges[0])
	}
	if ranges[1].Start != 0 || ranges[1].End != 2*60+30 {
		t.Fatalf("night second segment = %+v", ranges[1])
	}
	if ranges[2].Start != 9*60 || ranges[2].End != 10*60+15 {
		t.Fatalf("day first segment = %+v", ranges[2])
	}
}
