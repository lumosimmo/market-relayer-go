package feeds

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

func TestSourceBuilderHelpers(t *testing.T) {
	t.Parallel()

	t.Run("sorts kinds and builds registered adapters", func(t *testing.T) {
		t.Parallel()

		var called bool
		builders := map[string]sourceBuilder{
			"zeta": func(symbol string, source config.SourceConfig, options HTTPSourceOptions) (Source, error) {
				called = true
				return &stubSource{name: source.Name}, nil
			},
			"alpha": func(symbol string, source config.SourceConfig, options HTTPSourceOptions) (Source, error) {
				return &stubSource{name: symbol + ":" + source.Name}, nil
			},
		}

		if got, want := builderKinds(builders), []string{"alpha", "zeta"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("builderKinds(source) = %v, want %v", got, want)
		}

		source, err := buildSourceFrom(builders, "ETHUSD", config.SourceConfig{Name: "test", Kind: "zeta"}, HTTPSourceOptions{})
		if err != nil {
			t.Fatalf("buildSourceFrom() error = %v", err)
		}
		if !called {
			t.Fatal("buildSourceFrom() did not invoke registered builder")
		}
		if got, want := source.Name(), "test"; got != want {
			t.Fatalf("source.Name() = %q, want %q", got, want)
		}
	})

	t.Run("reports unsupported kinds", func(t *testing.T) {
		t.Parallel()

		if _, err := buildSourceFrom(map[string]sourceBuilder{
			config.SourceKindCoinbase: NewCoinbaseSource,
		}, "ETHUSD", config.SourceConfig{Kind: "missing"}, HTTPSourceOptions{}); err == nil {
			t.Fatal("buildSourceFrom(unsupported) error = nil, want error")
		}
		if got := builderKinds(map[string]sourceBuilder(nil)); got != nil {
			t.Fatalf("builderKinds(nil source builders) = %v, want nil", got)
		}
	})
}

func TestBookSourceBuilderHelpers(t *testing.T) {
	t.Parallel()

	t.Run("sorts kinds and builds registered adapters", func(t *testing.T) {
		t.Parallel()

		var called bool
		builders := map[string]bookSourceBuilder{
			"zeta": func(symbol string, source config.SourceConfig, options HTTPSourceOptions) (BookSource, error) {
				called = true
				return &stubBookSource{name: source.Name}, nil
			},
			"alpha": func(symbol string, source config.SourceConfig, options HTTPSourceOptions) (BookSource, error) {
				return &stubBookSource{name: symbol + ":" + source.Name}, nil
			},
		}

		if got, want := builderKinds(builders), []string{"alpha", "zeta"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("builderKinds(book) = %v, want %v", got, want)
		}

		source, err := buildBookSourceFrom(builders, "WTIUSD", config.SourceConfig{Name: "book", Kind: "zeta"}, HTTPSourceOptions{})
		if err != nil {
			t.Fatalf("buildBookSourceFrom() error = %v", err)
		}
		if !called {
			t.Fatal("buildBookSourceFrom() did not invoke registered builder")
		}
		if got, want := source.Name(), "book"; got != want {
			t.Fatalf("source.Name() = %q, want %q", got, want)
		}
	})

	t.Run("reports unsupported kinds", func(t *testing.T) {
		t.Parallel()

		if _, err := buildBookSourceFrom(map[string]bookSourceBuilder{
			config.SourceKindHyperliquid: NewHyperliquidBookSource,
		}, "WTIUSD", config.SourceConfig{Kind: "missing"}, HTTPSourceOptions{}); err == nil {
			t.Fatal("buildBookSourceFrom(unsupported) error = nil, want error")
		}
		if got := builderKinds(map[string]bookSourceBuilder(nil)); got != nil {
			t.Fatalf("builderKinds(nil book builders) = %v, want nil", got)
		}
	})
}

func TestSourceByName(t *testing.T) {
	t.Parallel()

	sources := []Source{
		&stubSource{name: "alpha"},
		&stubSource{name: "beta"},
	}

	source, ok := SourceByName(sources, "beta")
	if !ok {
		t.Fatal("SourceByName(existing) = false, want true")
	}
	if got := source.Name(); got != "beta" {
		t.Fatalf("SourceByName(existing).Name() = %q, want %q", got, "beta")
	}
	if _, ok := SourceByName(sources, "missing"); ok {
		t.Fatal("SourceByName(missing) = true, want false")
	}
}

func TestNewHTTPSourceFromSpec(t *testing.T) {
	t.Parallel()

	resolveErr := errors.New("resolve failed")
	wantQuote := QuoteInput{
		Source:        "custom",
		Symbol:        "ETHUSD",
		Bid:           "100.0",
		Ask:           "100.2",
		Last:          "100.1",
		ReceivedAt:    time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC),
		TimestampMode: config.TimestampModeReceivedAt,
	}

	t.Run("propagates resolve errors", func(t *testing.T) {
		t.Parallel()

		_, err := newHTTPSourceFromSpec("ETHUSD", config.SourceConfig{
			Name:           "custom",
			Kind:           "custom",
			RequestTimeout: time.Second,
		}, HTTPSourceOptions{}, httpSourceSpec{
			DefaultBaseURL: "http://example.com",
			ResolveURL: func(config.SourceConfig, string) (string, error) {
				return "", resolveErr
			},
			Decode: func(string, config.SourceConfig, time.Time, []byte) (QuoteInput, error) {
				return QuoteInput{}, nil
			},
		})
		if !errors.Is(err, resolveErr) {
			t.Fatalf("newHTTPSourceFromSpec() error = %v, want %v", err, resolveErr)
		}
	})

	t.Run("uses default base url and decoder", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path != "/prices" {
				http.NotFound(writer, request)
				return
			}
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte(`ok`))
		}))
		defer server.Close()

		var gotBaseURL string
		var decodeCalled bool
		sourceIface, err := newHTTPSourceFromSpec("ETHUSD", config.SourceConfig{
			Name:           "custom",
			Kind:           "custom",
			RequestTimeout: time.Second,
			TimestampMode:  config.TimestampModeReceivedAt,
		}, HTTPSourceOptions{
			Client: server.Client(),
			Clock:  &mutableClock{now: wantQuote.ReceivedAt},
		}, httpSourceSpec{
			DefaultBaseURL: server.URL,
			ResolveURL: func(source config.SourceConfig, baseURL string) (string, error) {
				gotBaseURL = baseURL
				return baseURL + "/prices", nil
			},
			Decode: func(symbol string, source config.SourceConfig, receivedAt time.Time, body []byte) (QuoteInput, error) {
				decodeCalled = true
				return wantQuote, nil
			},
		})
		if err != nil {
			t.Fatalf("newHTTPSourceFromSpec() error = %v", err)
		}
		if gotBaseURL != server.URL {
			t.Fatalf("resolved base url = %q, want %q", gotBaseURL, server.URL)
		}

		httpSource, ok := sourceIface.(*httpSource)
		if !ok {
			t.Fatalf("newHTTPSourceFromSpec() type = %T, want *httpSource", sourceIface)
		}
		if httpSource.Name() != "custom" {
			t.Fatalf("source.Name() = %q, want custom", httpSource.Name())
		}
		quote, err := httpSource.Fetch(context.Background(), FetchBudget{})
		if err != nil {
			t.Fatalf("Fetch() error = %v", err)
		}
		if !decodeCalled {
			t.Fatal("Fetch() did not call decoder")
		}
		if quote.Source != wantQuote.Source || quote.Symbol != wantQuote.Symbol {
			t.Fatalf("Fetch() quote = %+v, want source=%q symbol=%q", quote, wantQuote.Source, wantQuote.Symbol)
		}
	})
}

type stubSource struct {
	name string
}

func (source *stubSource) Name() string {
	return source.name
}

func (*stubSource) Fetch(context.Context, FetchBudget) (Quote, error) {
	return Quote{}, nil
}

type stubBookSource struct {
	name string
}

func (source *stubBookSource) Name() string {
	return source.name
}

func (*stubBookSource) FetchBook(context.Context, FetchBudget) (BookSnapshot, error) {
	return BookSnapshot{}, nil
}
