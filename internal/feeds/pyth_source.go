package feeds

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/markettime"
)

const (
	pythDefaultBaseURL          = "https://hermes.pyth.network"
	pythMetadataRefreshInterval = 15 * time.Minute
)

type pythSource struct {
	config             config.SourceConfig
	symbol             string
	clock              clock.Clock
	logger             *slog.Logger
	requester          *httpRequester
	baseURL            string
	stalenessThreshold time.Duration
	maxFutureSkew      time.Duration

	mu                  sync.RWMutex
	contracts           []pythContract
	nextMetadataRefresh time.Time
}

type pythContract struct {
	id            string
	genericSymbol string
	description   string
	expiryDate    time.Time
	schedule      markettime.Schedule
}

type pythMetadataResponse struct {
	ID         string `json:"id"`
	Attributes struct {
		AssetType     string `json:"asset_type"`
		Description   string `json:"description"`
		GenericSymbol string `json:"generic_symbol"`
		QuoteCurrency string `json:"quote_currency"`
		Schedule      string `json:"schedule"`
	} `json:"attributes"`
}

type pythLatestResponse struct {
	Parsed []pythLatestPrice `json:"parsed"`
}

type pythLatestPrice struct {
	ID    string          `json:"id"`
	Price pythLatestValue `json:"price"`
}

type pythLatestValue struct {
	Price       string `json:"price"`
	Conf        string `json:"conf"`
	Expo        int32  `json:"expo"`
	PublishTime int64  `json:"publish_time"`
}

type weightedContract struct {
	contract  pythContract
	weightBPS int64
}

type liveContract struct {
	contract    pythContract
	weightBPS   int64
	price       fixedpoint.Value
	conf        fixedpoint.Value
	publishTime time.Time
}

func NewPythSource(symbol string, source config.SourceConfig, options HTTPSourceOptions) (Source, error) {
	if strings.TrimSpace(symbol) == "" {
		return nil, fmt.Errorf("feeds: symbol is required")
	}
	if strings.TrimSpace(source.Name) == "" {
		return nil, fmt.Errorf("feeds: source name is required")
	}
	if options.MaxFutureSkew <= 0 {
		options.MaxFutureSkew = DefaultMaxFutureSkew
	}

	baseURL := options.BaseURL
	if baseURL == "" {
		baseURL = pythDefaultBaseURL
	}

	serviceClock := options.Clock
	if serviceClock == nil {
		serviceClock = clock.Real{}
	}

	return &pythSource{
		config:             source,
		symbol:             symbol,
		clock:              serviceClock,
		logger:             options.Logger,
		requester:          newHTTPRequester(source, options),
		baseURL:            baseURL,
		stalenessThreshold: options.StalenessThreshold,
		maxFutureSkew:      options.MaxFutureSkew,
	}, nil
}

func (source *pythSource) Name() string {
	return source.config.Name
}

func (source *pythSource) SessionOpenAt(at time.Time) bool {
	contracts := source.cachedContracts()
	if len(contracts) == 0 {
		return true
	}

	for _, current := range source.selectWeightedContracts(at, contracts) {
		if current.weightBPS > 0 && current.contract.schedule.OpenAt(at) {
			return true
		}
	}
	return false
}

func (source *pythSource) Fetch(ctx context.Context, budget FetchBudget) (Quote, error) {
	contracts, err := source.ensureContracts(ctx, budget)
	if err != nil {
		return Quote{}, err
	}

	now := source.clock.Now().UTC()
	selected := source.selectWeightedContracts(now, contracts)
	if len(selected) == 0 {
		return Quote{}, invalidQuoteError(source.config.Name, "no active pyth contracts available", nil)
	}

	latestByID, receivedAt, err := source.fetchLatestPrices(ctx, budget, selected)
	if err != nil {
		return Quote{}, err
	}

	live, err := source.liveContracts(now, selected, latestByID)
	if err != nil {
		return Quote{}, err
	}

	renormalized, err := renormalizeLiveContracts(live)
	if err != nil {
		return Quote{}, err
	}
	if source.logger != nil {
		source.logger.Debug(
			"pyth contract selection",
			"source", source.config.Name,
			"market", source.symbol,
			"contracts", contractSelectionFields(renormalized),
		)
	}

	return source.syntheticQuote(receivedAt, renormalized)
}
