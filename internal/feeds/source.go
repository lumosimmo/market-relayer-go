package feeds

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
)

const DefaultMaxFutureSkew = 2 * time.Second

type Source interface {
	Name() string
	Fetch(ctx context.Context, budget FetchBudget) (Quote, error)
}

type BookSource interface {
	Name() string
	FetchBook(ctx context.Context, budget FetchBudget) (BookSnapshot, error)
}

type SessionAwareSource interface {
	SessionOpenAt(time.Time) bool
}

type FetchBudget struct {
	Deadline time.Time
}

type FetchResult struct {
	Source string
	Quote  Quote
	Err    error
}

type BookFetchResult struct {
	Source string
	Book   BookSnapshot
	Err    error
}

type HTTPSourceOptions struct {
	BaseURL            string
	Client             *http.Client
	Clock              clock.Clock
	Logger             *slog.Logger
	StalenessThreshold time.Duration
	MaxFutureSkew      time.Duration
	CooldownJitter     func(time.Duration) time.Duration
}

type BuildOptions struct {
	Clock             clock.Clock
	Client            *http.Client
	Logger            *slog.Logger
	EndpointOverrides map[string]string
	MaxFutureSkew     time.Duration
	CooldownJitter    func(time.Duration) time.Duration
}

func NewSource(symbol string, source config.SourceConfig, options HTTPSourceOptions) (Source, error) {
	return buildSourceFrom(sourceBuilders, symbol, source, options)
}

func BuildMarketSources(cfg *config.Config, market config.MarketConfig, options BuildOptions) ([]Source, error) {
	sources := make([]Source, 0, len(market.Sources))
	for _, ref := range market.Sources {
		sourceConfig, ok := cfg.SourceByName(ref.Name)
		if !ok {
			return nil, fmt.Errorf("feeds: unknown source %q for market %s", ref.Name, market.Symbol)
		}

		baseURL := ""
		if options.EndpointOverrides != nil {
			baseURL = options.EndpointOverrides[sourceConfig.Name]
		}

		source, err := NewSource(market.Symbol, sourceConfig, HTTPSourceOptions{
			BaseURL:            baseURL,
			Client:             options.Client,
			Clock:              options.Clock,
			Logger:             options.Logger,
			StalenessThreshold: market.StalenessThreshold,
			MaxFutureSkew:      options.MaxFutureSkew,
			CooldownJitter:     options.CooldownJitter,
		})
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}

	return sources, nil
}

func BuildPerpBookSource(cfg *config.Config, market config.MarketConfig, options BuildOptions) (BookSource, error) {
	sourceConfig, ok := cfg.SourceByName(market.PerpBookSource)
	if !ok {
		return nil, fmt.Errorf("feeds: unknown perp book source %q for market %s", market.PerpBookSource, market.Symbol)
	}

	baseURL := ""
	if options.EndpointOverrides != nil {
		baseURL = options.EndpointOverrides[sourceConfig.Name]
	}

	source, err := buildBookSourceFrom(bookSourceBuilders, market.Symbol, sourceConfig, HTTPSourceOptions{
		BaseURL:            baseURL,
		Client:             options.Client,
		Clock:              options.Clock,
		Logger:             options.Logger,
		StalenessThreshold: market.StalenessThreshold,
		MaxFutureSkew:      options.MaxFutureSkew,
		CooldownJitter:     options.CooldownJitter,
	})
	if err != nil {
		return nil, err
	}
	return source, nil
}

func SourceByName(sources []Source, name string) (Source, bool) {
	index := slices.IndexFunc(sources, func(source Source) bool {
		return source.Name() == name
	})
	if index >= 0 {
		return sources[index], true
	}
	return nil, false
}

func FetchAll(ctx context.Context, sources []Source, budget FetchBudget) []FetchResult {
	results := make([]FetchResult, len(sources))

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(sources))
	for index, source := range sources {
		index := index
		source := source
		go func() {
			defer waitGroup.Done()

			quote, err := source.Fetch(ctx, budget)
			results[index] = FetchResult{
				Source: source.Name(),
				Quote:  quote,
				Err:    err,
			}
		}()
	}
	waitGroup.Wait()

	return results
}

func FetchBook(ctx context.Context, source BookSource, budget FetchBudget) BookFetchResult {
	if source == nil {
		return BookFetchResult{}
	}

	book, err := source.FetchBook(ctx, budget)
	return BookFetchResult{
		Source: source.Name(),
		Book:   book,
		Err:    err,
	}
}
