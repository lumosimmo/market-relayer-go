package feeds

import (
	"fmt"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
)

type FreshnessBasisKind string

const (
	FreshnessBasisSourceTimestamp FreshnessBasisKind = "source_ts"
	FreshnessBasisReceivedAt      FreshnessBasisKind = "received_at"
)

type Quote struct {
	Source             string
	Symbol             string
	Bid                fixedpoint.Value
	Ask                fixedpoint.Value
	Last               fixedpoint.Value
	Scale              int32
	ReceivedAt         time.Time
	SourceTS           *time.Time
	FreshnessBasisKind FreshnessBasisKind
	FreshnessBasisAt   time.Time
}

func (quote Quote) StableKey() string {
	return fmt.Sprintf(
		"%s|%s|%d|%d|%d|%d",
		quote.Source,
		quote.Symbol,
		quote.Scale,
		quote.Bid.Int,
		quote.Ask.Int,
		quote.Last.Int,
	)
}

type QuoteInput struct {
	Source        string
	Symbol        string
	Bid           string
	Ask           string
	Last          string
	ReceivedAt    time.Time
	SourceTS      *time.Time
	TimestampMode config.TimestampMode
}

type ValidationOptions struct {
	StalenessThreshold time.Duration
	MaxFutureSkew      time.Duration
}

func NormalizeQuote(input QuoteInput, options ValidationOptions) (Quote, error) {
	source := strings.TrimSpace(input.Source)
	symbol := strings.TrimSpace(input.Symbol)
	if source == "" {
		return Quote{}, invalidQuoteError(source, "source name is required", nil)
	}
	if symbol == "" {
		return Quote{}, invalidQuoteError(source, "symbol is required", nil)
	}
	if input.ReceivedAt.IsZero() {
		return Quote{}, invalidQuoteError(source, "received_at is required", nil)
	}

	bid, err := parseQuoteValue(source, "bid", input.Bid)
	if err != nil {
		return Quote{}, err
	}
	ask, err := parseQuoteValue(source, "ask", input.Ask)
	if err != nil {
		return Quote{}, err
	}
	last, err := parseQuoteValue(source, "last", input.Last)
	if err != nil {
		return Quote{}, err
	}

	targetScale := max(bid.Scale, max(ask.Scale, last.Scale))
	bid, err = normalizeQuoteValue(source, "bid", bid, targetScale)
	if err != nil {
		return Quote{}, err
	}
	ask, err = normalizeQuoteValue(source, "ask", ask, targetScale)
	if err != nil {
		return Quote{}, err
	}
	last, err = normalizeQuoteValue(source, "last", last, targetScale)
	if err != nil {
		return Quote{}, err
	}

	if bid.Int <= 0 || ask.Int <= 0 || last.Int <= 0 {
		return Quote{}, invalidQuoteError(source, "quotes must be positive", nil)
	}
	if bid.Int > ask.Int {
		return Quote{}, invalidQuoteError(source, "bid exceeds ask", nil)
	}

	receivedAt := input.ReceivedAt.UTC()
	quote := Quote{
		Source:     source,
		Symbol:     symbol,
		Bid:        bid,
		Ask:        ask,
		Last:       last,
		Scale:      targetScale,
		ReceivedAt: receivedAt,
		SourceTS:   normalizeTimestamp(input.SourceTS),
	}

	return applyFreshnessBasis(quote, input.TimestampMode, options)
}

func parseQuoteValue(source string, field string, input string) (fixedpoint.Value, error) {
	value, err := fixedpoint.ParseDecimal(input)
	if err != nil {
		return fixedpoint.Value{}, invalidQuoteError(source, "invalid "+field, err)
	}
	return value, nil
}

func normalizeQuoteValue(source string, field string, value fixedpoint.Value, targetScale int32) (fixedpoint.Value, error) {
	normalized, err := fixedpoint.Normalize(value, targetScale)
	if err != nil {
		return fixedpoint.Value{}, invalidQuoteError(source, "normalize "+field, err)
	}
	return normalized, nil
}

func normalizeTimestamp(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

func applyFreshnessBasis(quote Quote, mode config.TimestampMode, options ValidationOptions) (Quote, error) {
	switch mode {
	case config.TimestampModeSourceTimestamp:
		if quote.SourceTS == nil {
			return Quote{}, invalidQuoteError(quote.Source, "source timestamp is required", nil)
		}
		age := quote.ReceivedAt.Sub(*quote.SourceTS)
		if options.StalenessThreshold > 0 && age > options.StalenessThreshold {
			return Quote{}, staleQuoteError(quote.Source, age)
		}
		skew := quote.SourceTS.Sub(quote.ReceivedAt)
		if options.MaxFutureSkew > 0 && skew > options.MaxFutureSkew {
			return Quote{}, futureQuoteError(quote.Source, skew)
		}
		quote.FreshnessBasisKind = FreshnessBasisSourceTimestamp
		quote.FreshnessBasisAt = *quote.SourceTS
	case config.TimestampModeReceivedAt:
		quote.FreshnessBasisKind = FreshnessBasisReceivedAt
		quote.FreshnessBasisAt = quote.ReceivedAt
	default:
		return Quote{}, invalidQuoteError(quote.Source, fmt.Sprintf("unsupported timestamp mode %q", mode), nil)
	}

	return quote, nil
}
