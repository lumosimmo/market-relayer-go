package feeds

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
)

const defaultHyperliquidBaseURL = "https://api.hyperliquid.xyz"

type hyperliquidBookSource struct {
	config             config.SourceConfig
	symbol             string
	requestURL         string
	requester          *httpRequester
	stalenessThreshold time.Duration
	maxFutureSkew      time.Duration
}

func NewHyperliquidBookSource(symbol string, source config.SourceConfig, options HTTPSourceOptions) (BookSource, error) {
	requestURL, err := resolveHyperliquidURL(source, options.BaseURL)
	if err != nil {
		return nil, err
	}
	if options.MaxFutureSkew <= 0 {
		options.MaxFutureSkew = DefaultMaxFutureSkew
	}

	return &hyperliquidBookSource{
		config:             source,
		symbol:             symbol,
		requestURL:         requestURL,
		requester:          newHTTPRequester(source, options),
		stalenessThreshold: options.StalenessThreshold,
		maxFutureSkew:      options.MaxFutureSkew,
	}, nil
}

func (source *hyperliquidBookSource) Name() string {
	return source.config.Name
}

func (source *hyperliquidBookSource) FetchBook(ctx context.Context, budget FetchBudget) (BookSnapshot, error) {
	requestBody, err := json.Marshal(struct {
		Type string `json:"type"`
		Coin string `json:"coin"`
	}{
		Type: "l2Book",
		Coin: source.config.Market,
	})
	if err != nil {
		return BookSnapshot{}, malformedPayloadError(source.config.Name, fmt.Errorf("encode request body: %w", err))
	}

	body, receivedAt, err := source.requester.postJSON(ctx, budget, source.requestURL, requestBody)
	if err != nil {
		return BookSnapshot{}, err
	}

	var payload struct {
		Coin   string `json:"coin"`
		TimeMS int64  `json:"time"`
		Levels [2][]struct {
			Price string `json:"px"`
			Size  string `json:"sz"`
		} `json:"levels"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return BookSnapshot{}, malformedPayloadError(source.config.Name, err)
	}
	if payload.Coin == "" {
		return BookSnapshot{}, malformedPayloadError(source.config.Name, fmt.Errorf("missing coin"))
	}
	if payload.TimeMS <= 0 {
		return BookSnapshot{}, malformedPayloadError(source.config.Name, fmt.Errorf("missing time"))
	}

	sourceTS := time.UnixMilli(payload.TimeMS).UTC()
	book := BookSnapshot{
		Source:     source.config.Name,
		Symbol:     source.symbol,
		Market:     payload.Coin,
		Bids:       make([]BookLevel, 0, len(payload.Levels[0])),
		Asks:       make([]BookLevel, 0, len(payload.Levels[1])),
		ReceivedAt: receivedAt.UTC(),
		SourceTS:   &sourceTS,
	}

	for _, current := range payload.Levels[0] {
		level, err := parseBookLevel(source.config.Name, current.Price, current.Size)
		if err != nil {
			return BookSnapshot{}, err
		}
		book.Bids = append(book.Bids, level)
	}
	for _, current := range payload.Levels[1] {
		level, err := parseBookLevel(source.config.Name, current.Price, current.Size)
		if err != nil {
			return BookSnapshot{}, err
		}
		book.Asks = append(book.Asks, level)
	}

	return applyBookFreshnessBasis(book, source.stalenessThreshold, source.maxFutureSkew)
}

func resolveHyperliquidURL(source config.SourceConfig, baseURL string) (string, error) {
	if baseURL == "" {
		baseURL = defaultHyperliquidBaseURL
	}
	return resolveURL(baseURL, "/info", nil)
}

func parseBookLevel(source string, priceRaw string, sizeRaw string) (BookLevel, error) {
	price, err := fixedpoint.ParseDecimal(priceRaw)
	if err != nil {
		return BookLevel{}, invalidQuoteError(source, "invalid book price", err)
	}
	size, err := fixedpoint.ParseDecimal(sizeRaw)
	if err != nil {
		return BookLevel{}, invalidQuoteError(source, "invalid book size", err)
	}
	if price.Int <= 0 || size.Int <= 0 {
		return BookLevel{}, invalidQuoteError(source, "book levels must be positive", nil)
	}
	return BookLevel{Price: price, Size: size}, nil
}

func applyBookFreshnessBasis(book BookSnapshot, stalenessThreshold time.Duration, maxFutureSkew time.Duration) (BookSnapshot, error) {
	if book.SourceTS == nil {
		return BookSnapshot{}, invalidQuoteError(book.Source, "source timestamp is required", nil)
	}

	age := book.ReceivedAt.Sub(*book.SourceTS)
	if stalenessThreshold > 0 && age > stalenessThreshold {
		return BookSnapshot{}, staleQuoteError(book.Source, age)
	}

	skew := book.SourceTS.Sub(book.ReceivedAt)
	if maxFutureSkew > 0 && skew > maxFutureSkew {
		return BookSnapshot{}, futureQuoteError(book.Source, skew)
	}

	book.FreshnessBasisKind = FreshnessBasisSourceTimestamp
	book.FreshnessBasisAt = *book.SourceTS
	return book, nil
}
