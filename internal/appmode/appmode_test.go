package appmode

import "testing"

func TestNormalize(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"":             LiveReal,
		"live_real":    LiveReal,
		"live_paper":   LivePaper,
		"replay_paper": ReplayPaper,
		"unknown":      LiveReal,
	}
	for input, want := range tests {
		if got := Normalize(input); got != want {
			t.Fatalf("Normalize(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestUsesRealtimeMarket(t *testing.T) {
	t.Parallel()
	if !UsesRealtimeMarket(LiveReal) {
		t.Fatal("UsesRealtimeMarket(live_real) = false, want true")
	}
	if !UsesRealtimeMarket(LivePaper) {
		t.Fatal("UsesRealtimeMarket(live_paper) = false, want true")
	}
	if UsesRealtimeMarket(ReplayPaper) {
		t.Fatal("UsesRealtimeMarket(replay_paper) = true, want false")
	}
}
