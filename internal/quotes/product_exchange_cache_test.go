package quotes

import "testing"

func TestProductExchangeCacheResolveCanonicalCaseAndExchange(t *testing.T) {
	t.Parallel()

	cache := NewProductExchangeCache()
	cache.Replace([]ProductExchangeRecord{
		{ProductID: "SR", ProductIDNorm: "sr", ExchangeID: "CZCE"},
	})

	resolved, err := cache.Resolve("sr2509", "")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.Symbol != "SR2509" {
		t.Fatalf("resolved.Symbol = %q, want SR2509", resolved.Symbol)
	}
	if resolved.ExchangeID != "CZCE" {
		t.Fatalf("resolved.ExchangeID = %q, want CZCE", resolved.ExchangeID)
	}
}

func TestProductExchangeCacheResolveRequiresExchangeWhenAmbiguous(t *testing.T) {
	t.Parallel()

	cache := NewProductExchangeCache()
	cache.Replace([]ProductExchangeRecord{
		{ProductID: "AP", ProductIDNorm: "ap", ExchangeID: "CZCE"},
		{ProductID: "AP", ProductIDNorm: "ap", ExchangeID: "GFEX"},
	})

	if _, err := cache.Resolve("ap2509", ""); err == nil {
		t.Fatal("Resolve() error = nil, want ambiguity error")
	}
}
