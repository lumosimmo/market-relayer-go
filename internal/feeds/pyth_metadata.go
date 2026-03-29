package feeds

import (
	"context"
	"encoding/json"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/markettime"
)

func (source *pythSource) ensureContracts(ctx context.Context, budget FetchBudget) ([]pythContract, error) {
	now := source.clock.Now().UTC()
	source.mu.RLock()
	if len(source.contracts) > 0 && now.Before(source.nextMetadataRefresh) {
		cached := slices.Clone(source.contracts)
		source.mu.RUnlock()
		return cached, nil
	}
	cached := slices.Clone(source.contracts)
	source.mu.RUnlock()

	metadataURL, err := resolveURL(source.baseURL, "/v2/price_feeds", url.Values{
		"query": []string{source.config.RootSymbol},
	})
	if err != nil {
		return nil, invalidQuoteError(source.config.Name, "resolve pyth metadata url", err)
	}

	var payload []pythMetadataResponse
	if _, err := source.fetchJSON(ctx, budget, metadataURL, &payload); err != nil {
		if len(cached) > 0 {
			if source.logger != nil {
				source.logger.Warn("pyth metadata refresh failed; using cached contracts", "source", source.config.Name, "err", err)
			}
			return cached, nil
		}
		return nil, err
	}

	contracts, err := source.parseContracts(payload)
	if err != nil {
		return nil, err
	}

	source.mu.Lock()
	source.contracts = slices.Clone(contracts)
	source.nextMetadataRefresh = now.Add(pythMetadataRefreshInterval)
	source.mu.Unlock()

	if source.logger != nil {
		source.logger.Info("pyth metadata refreshed", "source", source.config.Name, "contracts", len(contracts), "root_symbol", source.config.RootSymbol)
	}

	return contracts, nil
}

func (source *pythSource) cachedContracts() []pythContract {
	source.mu.RLock()
	defer source.mu.RUnlock()
	return slices.Clone(source.contracts)
}

func (source *pythSource) fetchJSON(ctx context.Context, budget FetchBudget, requestURL string, target any) (time.Time, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		body, receivedAt, err := source.requester.get(ctx, budget, requestURL)
		if err != nil {
			lastErr = err
			if attempt == maxFetchAttempts-1 || !IsRetryable(err) || !source.requester.canRetry(budget) {
				return time.Time{}, lastErr
			}
			continue
		}
		if err := json.Unmarshal(body, target); err != nil {
			return time.Time{}, malformedPayloadError(source.config.Name, err)
		}
		return receivedAt, nil
	}
}

func (source *pythSource) parseContracts(payload []pythMetadataResponse) ([]pythContract, error) {
	contracts := make([]pythContract, 0, len(payload))
	for _, current := range payload {
		if !source.includeContract(current) {
			continue
		}

		schedule, err := markettime.ParseSchedule(current.Attributes.Schedule)
		if err != nil {
			return nil, invalidQuoteError(source.config.Name, "parse pyth schedule", err)
		}
		expiryDate, err := parsePythExpiry(current.Attributes.Description, schedule.Location())
		if err != nil {
			return nil, invalidQuoteError(source.config.Name, "parse pyth expiry", err)
		}
		contracts = append(contracts, pythContract{
			id:            current.ID,
			genericSymbol: current.Attributes.GenericSymbol,
			description:   current.Attributes.Description,
			expiryDate:    expiryDate,
			schedule:      schedule,
		})
	}

	if len(contracts) == 0 {
		return nil, invalidQuoteError(source.config.Name, "no matching pyth contracts found", nil)
	}

	slices.SortFunc(contracts, func(left, right pythContract) int {
		if !left.expiryDate.Equal(right.expiryDate) {
			if left.expiryDate.Before(right.expiryDate) {
				return -1
			}
			return 1
		}
		return strings.Compare(left.genericSymbol, right.genericSymbol)
	})

	return contracts, nil
}

func (source *pythSource) includeContract(metadata pythMetadataResponse) bool {
	description := strings.ToUpper(strings.TrimSpace(metadata.Attributes.Description))
	if strings.Contains(description, "DEPRECATED FEED") {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(metadata.Attributes.AssetType), source.config.AssetType) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(metadata.Attributes.QuoteCurrency), source.config.QuoteCurrency) {
		return false
	}
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(metadata.Attributes.GenericSymbol)), strings.ToUpper(source.config.RootSymbol))
}
