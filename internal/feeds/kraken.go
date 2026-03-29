package feeds

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

const defaultKrakenBaseURL = "https://api.kraken.com"

var krakenHTTPSourceSpec = httpSourceSpec{
	DefaultBaseURL: defaultKrakenBaseURL,
	ResolveURL: func(source config.SourceConfig, baseURL string) (string, error) {
		query := url.Values{}
		query.Set("pair", source.Pair)
		return resolveURL(baseURL, "/0/public/Ticker", query)
	},
	Decode: func(symbol string, source config.SourceConfig, receivedAt time.Time, body []byte) (QuoteInput, error) {
		var payload struct {
			Error  []string `json:"error"`
			Result map[string]struct {
				Ask  []string `json:"a"`
				Bid  []string `json:"b"`
				Last []string `json:"c"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return QuoteInput{}, malformedPayloadError(source.Name, err)
		}
		if len(payload.Error) > 0 {
			return QuoteInput{}, malformedPayloadError(source.Name, fmt.Errorf("kraken returned errors: %s", strings.Join(payload.Error, ", ")))
		}
		if len(payload.Result) == 0 {
			return QuoteInput{}, malformedPayloadError(source.Name, errors.New("kraken result was empty"))
		}

		keys := make([]string, 0, len(payload.Result))
		for key := range payload.Result {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		ticker := payload.Result[keys[0]]
		if len(ticker.Ask) == 0 || len(ticker.Bid) == 0 || len(ticker.Last) == 0 {
			return QuoteInput{}, malformedPayloadError(source.Name, errors.New("kraken result missing best bid/ask/last"))
		}

		return QuoteInput{
			Source:        source.Name,
			Symbol:        symbol,
			Bid:           ticker.Bid[0],
			Ask:           ticker.Ask[0],
			Last:          ticker.Last[0],
			ReceivedAt:    receivedAt,
			TimestampMode: source.TimestampMode,
		}, nil
	},
}

func NewKrakenSource(symbol string, source config.SourceConfig, options HTTPSourceOptions) (Source, error) {
	return newHTTPSourceFromSpec(symbol, source, options, krakenHTTPSourceSpec)
}
