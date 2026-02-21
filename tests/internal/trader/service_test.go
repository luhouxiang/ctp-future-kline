package trader_test

import (
	"reflect"
	"testing"

	"ctp-go-demo/tests/internal/trader/testkit"
	ctp "github.com/kkqy/ctp-go"
)

func TestSelectSubscribeTargetsAllDomesticFuturesWhenConfigEmpty(t *testing.T) {
	t.Parallel()

	queried := []testkit.InstrumentInfo{
		{ID: "rb2405", ExchangeID: "SHFE", ProductID: "rb", ProductClass: ctp.THOST_FTDC_PC_Futures},
		{ID: "m2409", ExchangeID: "DCE", ProductID: "m", ProductClass: ctp.THOST_FTDC_PC_Futures},
		{ID: "sc2406", ExchangeID: "INE", ProductID: "sc", ProductClass: ctp.THOST_FTDC_PC_Futures},
		{ID: "IF2403", ExchangeID: "CFFEX", ProductID: "IF", ProductClass: ctp.THOST_FTDC_PC_Futures},
		{ID: "rb2405", ExchangeID: "SHFE", ProductID: "rb", ProductClass: ctp.THOST_FTDC_PC_Futures},
		{ID: "au2406C400", ExchangeID: "SHFE", ProductID: "au", ProductClass: ctp.THOST_FTDC_PC_Options},
		{ID: "CL2405", ExchangeID: "NYMEX", ProductID: "CL", ProductClass: ctp.THOST_FTDC_PC_Futures},
		{ID: "", ExchangeID: "SHFE", ProductID: "rb", ProductClass: ctp.THOST_FTDC_PC_Futures},
	}

	got := testkit.SelectSubscribeTargets(queried, nil)
	want := []string{"rb2405", "m2409", "sc2406", "IF2403"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectSubscribeTargets() = %v, want %v", got, want)
	}
}

func TestSelectSubscribeTargetsFilterByConfiguredVariety(t *testing.T) {
	t.Parallel()

	queried := []testkit.InstrumentInfo{
		{ID: "rb2405", ExchangeID: "SHFE", ProductID: "rb", ProductClass: ctp.THOST_FTDC_PC_Futures},
		{ID: "rb2410", ExchangeID: "SHFE", ProductID: "rb", ProductClass: ctp.THOST_FTDC_PC_Futures},
		{ID: "m2409", ExchangeID: "DCE", ProductID: "m", ProductClass: ctp.THOST_FTDC_PC_Futures},
		{ID: "IF2403", ExchangeID: "CFFEX", ProductID: "IF", ProductClass: ctp.THOST_FTDC_PC_Futures},
		{ID: "au2406", ExchangeID: "SHFE", ProductID: "au", ProductClass: ctp.THOST_FTDC_PC_Futures},
	}

	got := testkit.SelectSubscribeTargets(queried, []string{" RB", "m2401", "if", "  "})
	want := []string{"rb2405", "rb2410", "m2409", "IF2403"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectSubscribeTargets() = %v, want %v", got, want)
	}
}

func TestNormalizeVariety(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{in: "rb2405", want: "rb"},
		{in: "SR605", want: "sr"},
		{in: "  IF  ", want: "if"},
		{in: "2405", want: ""},
		{in: "", want: ""},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			if got := testkit.NormalizeVariety(tc.in); got != tc.want {
				t.Fatalf("normalizeVariety(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
