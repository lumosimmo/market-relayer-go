package testutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func TestLoadFixtureSetAndScenarioLookupEdges(t *testing.T) {
	t.Parallel()

	t.Run("missing file and invalid yaml fail fast", func(t *testing.T) {
		t.Parallel()

		if _, err := LoadFixtureSet(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
			t.Fatal("LoadFixtureSet(missing file) error = nil, want read failure")
		}

		path := filepath.Join(t.TempDir(), "broken.yaml")
		writeFixtureFile(t, path, "name: [broken")
		if _, err := LoadFixtureSet(path); err == nil {
			t.Fatal("LoadFixtureSet(invalid yaml) error = nil, want decode failure")
		}
	})

	t.Run("top-level validation rejects missing required fields", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "fixture.yaml")
		writeFixtureFile(t, path, `metadata: {}
market:
  symbol: ETHUSD
  session_mode: always_open
  publish_interval: 3s
  staleness_threshold: 10s
  max_fallback_age: 30s
  clamp_bps: 50
  divergence_threshold_bps: 100
  publish_scale: 2
source_order:
  - coinbase
scenarios: []
`)
		if _, err := LoadFixtureSet(path); err == nil || !strings.Contains(err.Error(), "fixture set name is required") {
			t.Fatalf("LoadFixtureSet(missing name) error = %v, want missing-name failure", err)
		}

		writeFixtureFile(t, path, `name: fixture
metadata: {}
market:
  symbol: ETHUSD
  session_mode: always_open
  publish_interval: 3s
  staleness_threshold: 10s
  max_fallback_age: 30s
  clamp_bps: 50
  divergence_threshold_bps: 100
  publish_scale: 2
scenarios: []
`)
		if _, err := LoadFixtureSet(path); err == nil || !strings.Contains(err.Error(), "source_order is required") {
			t.Fatalf("LoadFixtureSet(missing source_order) error = %v, want source-order failure", err)
		}
	})

	t.Run("valid fixture set loads and scenario lookup returns false for misses", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "fixture.yaml")
		writeFixtureFile(t, path, validFixtureYAML())

		fixtures, err := LoadFixtureSet(path)
		if err != nil {
			t.Fatalf("LoadFixtureSet(valid) error = %v", err)
		}
		if fixtures.Name != "fixture-test" {
			t.Fatalf("FixtureSet.Name = %q, want fixture-test", fixtures.Name)
		}
		if fixtures.Metadata.PricingAlgorithmVersion != pricing.AlgorithmVersion {
			t.Fatalf("FixtureSet.Metadata = %+v, want pricing version", fixtures.Metadata)
		}
		if _, ok := fixtures.Scenario("missing"); ok {
			t.Fatal("Scenario(missing) = true, want false")
		}
	})
}

func TestFixtureNormalizationHelpersCoverValidationBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC)

	t.Run("market config rejects invalid durations and empty source names", func(t *testing.T) {
		t.Parallel()

		_, err := fixtureMarketFile{
			Symbol:             "ETHUSD",
			SessionMode:        "always_open",
			PublishInterval:    "bad",
			StalenessThreshold: "10s",
			MaxFallbackAge:     "30s",
			PublishScale:       2,
		}.marketConfig([]string{"coinbase"})
		if err == nil {
			t.Fatal("marketConfig(invalid duration) error = nil, want parse failure")
		}

		_, err = fixtureMarketFile{
			Symbol:             "ETHUSD",
			SessionMode:        "always_open",
			PublishInterval:    "3s",
			StalenessThreshold: "10s",
			MaxFallbackAge:     "30s",
			PublishScale:       2,
		}.marketConfig([]string{"coinbase", " "})
		if err == nil {
			t.Fatal("marketConfig(empty source name) error = nil, want validation failure")
		}
	})

	t.Run("scenario, quote, and sink normalization reject malformed inputs", func(t *testing.T) {
		t.Parallel()

		_, err := fixtureScenarioFile{}.normalize("ETHUSD")
		if err == nil {
			t.Fatal("fixtureScenarioFile.normalize(empty) error = nil, want validation failure")
		}
		_, err = fixtureScenarioFile{Name: "scenario"}.normalize("ETHUSD")
		if err == nil {
			t.Fatal("fixtureScenarioFile.normalize(missing record id) error = nil, want validation failure")
		}
		_, err = fixtureScenarioFile{Name: "scenario", RecordID: "cycle-1"}.normalize("ETHUSD")
		if err == nil {
			t.Fatal("fixtureScenarioFile.normalize(missing time) error = nil, want validation failure")
		}

		_, err = fixtureQuoteFile{Source: "coinbase", Mid: "bad", BasisKind: "source_ts", BasisAt: now}.quote("ETHUSD")
		if err == nil {
			t.Fatal("fixtureQuoteFile.quote(invalid mid) error = nil, want parse failure")
		}
		_, err = fixtureQuoteFile{Mid: "1981.75", BasisKind: "source_ts", BasisAt: now}.quote("ETHUSD")
		if err == nil {
			t.Fatal("fixtureQuoteFile.quote(missing source) error = nil, want validation failure")
		}
		_, err = fixtureQuoteFile{Source: "coinbase", Mid: "1981.75", BasisKind: "source_ts"}.quote("ETHUSD")
		if err == nil {
			t.Fatal("fixtureQuoteFile.quote(missing basis_at) error = nil, want validation failure")
		}
		_, err = fixtureQuoteFile{Source: "coinbase", Mid: "1981.75", BasisKind: "unsupported", BasisAt: now}.quote("ETHUSD")
		if err == nil {
			t.Fatal("fixtureQuoteFile.quote(unsupported basis kind) error = nil, want validation failure")
		}

		quote, err := fixtureQuoteFile{Source: "coinbase", Mid: "1981.75", BasisKind: "received_at", BasisAt: now}.quote("ETHUSD")
		if err != nil {
			t.Fatalf("fixtureQuoteFile.quote(received_at) error = %v", err)
		}
		if quote.SourceTS != nil {
			t.Fatalf("fixtureQuoteFile.quote(received_at).SourceTS = %v, want nil", quote.SourceTS)
		}

		_, err = fixtureSinkOutcomeFile{Kind: "ack"}.normalize()
		if err == nil {
			t.Fatal("fixtureSinkOutcomeFile.normalize(missing acked_at) error = nil, want validation failure")
		}
		_, err = fixtureSinkOutcomeFile{Kind: "unknown"}.normalize()
		if err == nil {
			t.Fatal("fixtureSinkOutcomeFile.normalize(unknown kind) error = nil, want validation failure")
		}
	})

	t.Run("duration and basis parsing cover aliases and invalid input", func(t *testing.T) {
		t.Parallel()

		if _, err := parseFixtureDuration("publish_interval", "bad"); err == nil {
			t.Fatal("parseFixtureDuration(invalid) error = nil, want parse failure")
		}
		if got, err := parseFixtureBasisKind("source_timestamp"); err != nil || got == "" {
			t.Fatalf("parseFixtureBasisKind(source_timestamp) = (%q, %v), want valid kind", got, err)
		}
		if got, err := parseFixtureBasisKind("source_ts"); err != nil || got == "" {
			t.Fatalf("parseFixtureBasisKind(source_ts) = (%q, %v), want valid kind", got, err)
		}
		if _, err := parseFixtureBasisKind("broken"); err == nil {
			t.Fatal("parseFixtureBasisKind(invalid) error = nil, want validation failure")
		}
	})
}

func writeFixtureFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
}

func validFixtureYAML() string {
	return `name: fixture-test
metadata:
  config_digest: config-digest
  pricing_algorithm_version: ` + pricing.AlgorithmVersion + `
  schema_version: ` + store.SchemaVersion + `
  envelope_version: ` + relayer.EnvelopeVersion + `
market:
  symbol: ETHUSD
  session_mode: always_open
  publish_interval: 3s
  staleness_threshold: 10s
  max_fallback_age: 30s
  clamp_bps: 50
  divergence_threshold_bps: 100
  publish_scale: 2
source_order:
  - coinbase
scenarios:
  - name: happy
    record_id: cycle-1
    at: 2026-03-27T12:00:00Z
    quotes:
      - source: coinbase
        mid: "1981.75"
        basis_kind: source_ts
        basis_at: 2026-03-27T12:00:00Z
    sink_outcomes:
      - kind: ack
        acked_at: 2026-03-27T12:00:01Z
`
}
