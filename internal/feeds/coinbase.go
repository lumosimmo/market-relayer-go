package feeds

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

const defaultCoinbaseBaseURL = "https://api.exchange.coinbase.com"

var coinbaseHTTPSourceSpec = httpSourceSpec{
	DefaultBaseURL: defaultCoinbaseBaseURL,
	ResolveURL: func(source config.SourceConfig, baseURL string) (string, error) {
		return resolveURL(baseURL, "/products/"+url.PathEscape(source.ProductID)+"/ticker", nil)
	},
	Decode: func(symbol string, source config.SourceConfig, receivedAt time.Time, body []byte) (QuoteInput, error) {
		var payload struct {
			Bid   string `json:"bid"`
			Ask   string `json:"ask"`
			Price string `json:"price"`
			Time  string `json:"time"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return QuoteInput{}, malformedPayloadError(source.Name, err)
		}

		var sourceTS *time.Time
		if payload.Time != "" {
			parsed, err := time.Parse(time.RFC3339Nano, payload.Time)
			if err != nil {
				return QuoteInput{}, malformedPayloadError(source.Name, fmt.Errorf("parse source timestamp: %w", err))
			}
			normalized := parsed.UTC()
			sourceTS = &normalized
		}

		return QuoteInput{
			Source:        source.Name,
			Symbol:        symbol,
			Bid:           payload.Bid,
			Ask:           payload.Ask,
			Last:          payload.Price,
			ReceivedAt:    receivedAt,
			SourceTS:      sourceTS,
			TimestampMode: source.TimestampMode,
		}, nil
	},
}

func NewCoinbaseSource(symbol string, source config.SourceConfig, options HTTPSourceOptions) (Source, error) {
	return newHTTPSourceFromSpec(symbol, source, options, coinbaseHTTPSourceSpec)
}
