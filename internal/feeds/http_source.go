package feeds

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

const maxFetchAttempts = 2

type httpSource struct {
	config             config.SourceConfig
	symbol             string
	requestURL         string
	requester          *httpRequester
	stalenessThreshold time.Duration
	maxFutureSkew      time.Duration
	decodeQuoteInput   quoteDecoder

	unchangedKey   string
	unchangedSince time.Time
	mu             sync.Mutex
}

func newHTTPSource(
	symbol string,
	source config.SourceConfig,
	requestURL string,
	options HTTPSourceOptions,
	decodeQuoteInput quoteDecoder,
) (Source, error) {
	if strings.TrimSpace(symbol) == "" {
		return nil, fmt.Errorf("feeds: symbol is required")
	}
	if strings.TrimSpace(source.Name) == "" {
		return nil, fmt.Errorf("feeds: source name is required")
	}
	if options.MaxFutureSkew <= 0 {
		options.MaxFutureSkew = DefaultMaxFutureSkew
	}

	return &httpSource{
		config:             source,
		symbol:             symbol,
		requestURL:         requestURL,
		requester:          newHTTPRequester(source, options),
		stalenessThreshold: options.StalenessThreshold,
		maxFutureSkew:      options.MaxFutureSkew,
		decodeQuoteInput:   decodeQuoteInput,
	}, nil
}

func (source *httpSource) Name() string {
	return source.config.Name
}

func (source *httpSource) Fetch(ctx context.Context, budget FetchBudget) (Quote, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		quote, err := source.fetchOnce(ctx, budget)
		if err == nil {
			return quote, nil
		}
		lastErr = err
		if attempt == maxFetchAttempts-1 || !IsRetryable(err) || !source.requester.canRetry(budget) {
			return Quote{}, lastErr
		}
	}
}

func (source *httpSource) fetchOnce(ctx context.Context, budget FetchBudget) (Quote, error) {
	body, receivedAt, err := source.requester.get(ctx, budget, source.requestURL)
	if err != nil {
		return Quote{}, err
	}

	input, err := source.decodeQuoteInput(receivedAt, body)
	if err != nil {
		return Quote{}, err
	}

	quote, err := NormalizeQuote(input, ValidationOptions{
		StalenessThreshold: source.stalenessThreshold,
		MaxFutureSkew:      source.maxFutureSkew,
	})
	if err != nil {
		return Quote{}, err
	}

	if source.config.TimestampMode == config.TimestampModeReceivedAt {
		if err := source.guardUnchangedQuote(quote); err != nil {
			return Quote{}, err
		}
	}

	return quote, nil
}

func (source *httpSource) guardUnchangedQuote(quote Quote) error {
	if source.config.MaxUnchangedAge <= 0 {
		return nil
	}

	source.mu.Lock()
	defer source.mu.Unlock()

	stableKey := quote.StableKey()
	if stableKey != source.unchangedKey {
		source.unchangedKey = stableKey
		source.unchangedSince = quote.ReceivedAt
		return nil
	}
	if source.unchangedSince.IsZero() {
		source.unchangedSince = quote.ReceivedAt
		return nil
	}

	unchangedFor := quote.ReceivedAt.Sub(source.unchangedSince)
	if unchangedFor > source.config.MaxUnchangedAge {
		return frozenQuoteError(source.config.Name, unchangedFor)
	}
	return nil
}

func resolveURL(baseURL string, path string, query url.Values) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	relative := &url.URL{
		Path: path,
	}
	if len(query) > 0 {
		relative.RawQuery = query.Encode()
	}

	return base.ResolveReference(relative).String(), nil
}
