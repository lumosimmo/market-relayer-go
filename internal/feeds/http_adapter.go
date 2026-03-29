package feeds

import (
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

type quoteDecoder func(time.Time, []byte) (QuoteInput, error)

type httpSourceSpec struct {
	DefaultBaseURL string
	ResolveURL     func(config.SourceConfig, string) (string, error)
	Decode         func(string, config.SourceConfig, time.Time, []byte) (QuoteInput, error)
}

func newHTTPSourceFromSpec(symbol string, source config.SourceConfig, options HTTPSourceOptions, spec httpSourceSpec) (Source, error) {
	baseURL := options.BaseURL
	if baseURL == "" {
		baseURL = spec.DefaultBaseURL
	}

	requestURL, err := spec.ResolveURL(source, baseURL)
	if err != nil {
		return nil, err
	}

	return newHTTPSource(symbol, source, requestURL, options, func(receivedAt time.Time, body []byte) (QuoteInput, error) {
		return spec.Decode(symbol, source, receivedAt, body)
	})
}
