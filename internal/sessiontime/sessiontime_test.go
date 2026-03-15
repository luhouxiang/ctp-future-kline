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
