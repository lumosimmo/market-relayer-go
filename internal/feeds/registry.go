package feeds

import (
	"fmt"
	"slices"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

type sourceBuilder func(string, config.SourceConfig, HTTPSourceOptions) (Source, error)
type bookSourceBuilder func(string, config.SourceConfig, HTTPSourceOptions) (BookSource, error)

var sourceBuilders = map[string]sourceBuilder{
	config.SourceKindCoinbase: NewCoinbaseSource,
	config.SourceKindKraken:   NewKrakenSource,
	config.SourceKindPyth:     NewPythSource,
}

var bookSourceBuilders = map[string]bookSourceBuilder{
	config.SourceKindHyperliquid: NewHyperliquidBookSource,
}

func buildSourceFrom(
	builders map[string]sourceBuilder,
	symbol string,
	source config.SourceConfig,
	options HTTPSourceOptions,
) (Source, error) {
	build, ok := builders[source.Kind]
	if !ok {
		return nil, fmt.Errorf("feeds: unsupported source kind %q", source.Kind)
	}
	return build(symbol, source, options)
}

func buildBookSourceFrom(
	builders map[string]bookSourceBuilder,
	symbol string,
	source config.SourceConfig,
	options HTTPSourceOptions,
) (BookSource, error) {
	build, ok := builders[source.Kind]
	if !ok {
		return nil, fmt.Errorf("feeds: unsupported book source kind %q", source.Kind)
	}
	return build(symbol, source, options)
}

func builderKinds[V any](builders map[string]V) []string {
	if len(builders) == 0 {
		return nil
	}

	kinds := make([]string, 0, len(builders))
	for kind := range builders {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	return kinds
}
