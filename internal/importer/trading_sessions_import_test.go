package importer

import "testing"

func TestIsTradingSessionCandidateFile(t *testing.T) {
	tests := []struct {
		name string
		file string
		want bool
	}{
		{name: "valid l9 txt", file: "30#AGL9.txt", want: true},
		{name: "valid path l9 txt", file: "flow/1m/30#AGL9.txt", want: true},
		{name: "valid lowercase l9 txt", file: "30#agl9.txt", want: true},
		{name: "reject calendar txt", file: "42#T033.txt", want: false},
		{name: "reject non l9 contract", file: "30#AG2605.txt", want: false},
		{name: "reject wrong suffix", file: "30#AGL9.csv", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTradingSessionCandidateFile(tc.file); got != tc.want {
				t.Fatalf("isTradingSessionCandidateFile(%q)=%v want %v", tc.file, got, tc.want)
			}
		})
	}
}

func TestBuildZeroBarsReason(t *testing.T) {
	tests := []struct {
		name      string
		lines     []string
		candidate int
		want      string
	}{
		{name: "daily header", lines: []string{"AGL9 白银加权 日线 不复权"}, candidate: 10, want: "expected 1m data but detected 日线 header"},
		{name: "5m header", lines: []string{"AGL9 白银加权 5分钟线 不复权"}, candidate: 10, want: "expected 1m data but detected 5分钟线 header"},
		{name: "no candidates", lines: []string{"AGL9 白银加权 1分钟线 不复权"}, candidate: 0, want: "no candidate data rows found"},
		{name: "generic", lines: []string{"AGL9 白银加权 1分钟线 不复权"}, candidate: 8, want: "no valid 1m rows parsed from candidate data lines"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildZeroBarsReason(tc.lines, tc.candidate); got != tc.want {
				t.Fatalf("buildZeroBarsReason()=%q want %q", got, tc.want)
			}
		})
	}
}
