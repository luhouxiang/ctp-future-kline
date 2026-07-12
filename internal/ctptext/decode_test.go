package ctptext

import (
	"testing"

	"golang.org/x/text/encoding/simplifiedchinese"
)

func TestDecodeGB18030CTPMessage(t *testing.T) {
	want := "不合法的登录"
	raw, err := simplifiedchinese.GB18030.NewEncoder().Bytes([]byte(want))
	if err != nil {
		t.Fatal(err)
	}
	if got := Decode(string(raw) + "\x00  "); got != want {
		t.Fatalf("Decode() = %q, want %q", got, want)
	}
}

func TestDecodePreservesUTF8(t *testing.T) {
	want := "CTP：前置未连接"
	if got := Decode("  " + want + "  "); got != want {
		t.Fatalf("Decode() = %q, want %q", got, want)
	}
}

func TestDecodePreservesASCII(t *testing.T) {
	if got := Decode("  request failed  "); got != "request failed" {
		t.Fatalf("Decode() = %q", got)
	}
}
