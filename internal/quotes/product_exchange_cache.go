package quotes

import (
	"fmt"
	"strings"
	"sync"

	dbx "ctp-future-kline/internal/db"
)

type ResolvedProductExchange struct {
	Symbol     string
	ExchangeID string
	Product    ProductExchangeRecord
}

type ProductExchangeCache struct {
	mu             sync.RWMutex
	byNormExchange map[string]ProductExchangeRecord
	byNorm         map[string][]ProductExchangeRecord
}

var defaultProductExchangeCache = NewProductExchangeCache()

func NewProductExchangeCache() *ProductExchangeCache {
	return &ProductExchangeCache{
		byNormExchange: make(map[string]ProductExchangeRecord),
		byNorm:         make(map[string][]ProductExchangeRecord),
	}
}

func DefaultProductExchangeCache() *ProductExchangeCache {
	return defaultProductExchangeCache
}

func (c *ProductExchangeCache) Count() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.byNormExchange)
}

func (c *ProductExchangeCache) EnsureLoadedFromDSN(dsn string) (int, error) {
	if c == nil {
		return 0, nil
	}
	if c.Count() > 0 {
		return c.Count(), nil
	}
	return c.LoadFromDSN(dsn)
}

func (c *ProductExchangeCache) LoadFromDSN(dsn string) (int, error) {
	if c == nil {
		return 0, nil
	}
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return 0, nil
	}
	db, err := dbx.Open(dsn)
	if err != nil {
		return 0, fmt.Errorf("open product exchange db failed: %w", err)
	}
	defer db.Close()
	repo := NewInstrumentCatalogRepo(db)
	items, err := repo.ListAllProductExchanges()
	if err != nil {
		return 0, err
	}
	c.Replace(items)
	return len(items), nil
}

func (c *ProductExchangeCache) Replace(items []ProductExchangeRecord) {
	if c == nil {
		return
	}
	byNormExchange := make(map[string]ProductExchangeRecord, len(items))
	byNorm := make(map[string][]ProductExchangeRecord)
	for _, item := range items {
		productIDNorm := normalizeProductID(item.ProductIDNorm)
		if productIDNorm == "" {
			productIDNorm = normalizeProductID(item.ProductID)
		}
		exchangeID := strings.TrimSpace(item.ExchangeID)
		if productIDNorm == "" || exchangeID == "" {
			continue
		}
		item.ProductIDNorm = productIDNorm
		byNormExchange[productExchangeKey(productIDNorm, exchangeID)] = item
		byNorm[productIDNorm] = append(byNorm[productIDNorm], item)
	}
	c.mu.Lock()
	c.byNormExchange = byNormExchange
	c.byNorm = byNorm
	c.mu.Unlock()
}

func (c *ProductExchangeCache) Resolve(symbol string, exchangeID string) (ResolvedProductExchange, error) {
	if c == nil {
		return ResolvedProductExchange{}, nil
	}
	productIDNorm, suffix := splitProductAndSuffix(symbol)
	if productIDNorm == "" {
		return ResolvedProductExchange{}, fmt.Errorf("symbol is required")
	}
	exchangeID = strings.TrimSpace(exchangeID)
	c.mu.RLock()
	defer c.mu.RUnlock()
	if exchangeID != "" {
		item, ok := c.byNormExchange[productExchangeKey(productIDNorm, exchangeID)]
		if !ok {
			return ResolvedProductExchange{}, fmt.Errorf("product %s not found on exchange %s", productIDNorm, exchangeID)
		}
		return ResolvedProductExchange{
			Symbol:     item.ProductID + suffix,
			ExchangeID: item.ExchangeID,
			Product:    item,
		}, nil
	}
	items := c.byNorm[productIDNorm]
	if len(items) == 0 {
		return ResolvedProductExchange{}, fmt.Errorf("product %s not found", productIDNorm)
	}
	if len(items) > 1 {
		return ResolvedProductExchange{}, fmt.Errorf("exchange_id is required for product %s", productIDNorm)
	}
	item := items[0]
	return ResolvedProductExchange{
		Symbol:     item.ProductID + suffix,
		ExchangeID: item.ExchangeID,
		Product:    item,
	}, nil
}

func (c *ProductExchangeCache) CanonicalizeProductCase(symbol string, exchangeID string) (string, error) {
	resolved, err := c.Resolve(symbol, exchangeID)
	if err != nil {
		return "", err
	}
	return resolved.Symbol, nil
}

func (c *ProductExchangeCache) InferExchangeByProduct(symbol string) (string, error) {
	resolved, err := c.Resolve(symbol, "")
	if err != nil {
		return "", err
	}
	return resolved.ExchangeID, nil
}

func splitProductAndSuffix(symbol string) (string, string) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return "", ""
	}
	idx := 0
	for _, ch := range symbol {
		if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') {
			idx++
			continue
		}
		break
	}
	if idx == 0 {
		return "", ""
	}
	return strings.ToLower(symbol[:idx]), symbol[idx:]
}

func productExchangeKey(productIDNorm string, exchangeID string) string {
	return normalizeProductID(productIDNorm) + "|" + strings.ToUpper(strings.TrimSpace(exchangeID))
}
