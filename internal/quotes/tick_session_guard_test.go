package quotes

import (
	"testing"

	"ctp-future-kline/internal/sessiontime"
)

func TestTickSessionDistanceMinutes(t *testing.T) {
	t.Parallel()

	sessions, err := sessiontime.ParseSessionText("21:00-02:30")
	if err != nil {
		t.Fatalf("ParseSessionText() error = %v", err)
	}

	cases := []struct {
		updateTime string
		want       int
	}{
		{updateTime: "21:00:00", want: 0},
		{updateTime: "20:58:59", want: 1},
		{updateTime: "02:33:01", want: 3},
		{updateTime: "18:38:00", want: 141},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.updateTime, func(t *testing.T) {
			t.Parallel()
			got, err := tickSessionDistanceMinutes(tc.updateTime, sessions)
			if err != nil {
				t.Fatalf("tickSessionDistanceMinutes(%q) error = %v", tc.updateTime, err)
			}
			if got != tc.want {
				t.Fatalf("tickSessionDistanceMinutes(%q) = %d, want %d", tc.updateTime, got, tc.want)
			}
		})
	}
}
