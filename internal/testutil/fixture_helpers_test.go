package testutil

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/lumosimmo/market-relayer-go/internal/operator"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func loadTestFixtures(tb testing.TB) FixtureSet {
	tb.Helper()

	path := filepath.Join("testdata", "verification-fixtures.yaml")
	fixtures, err := LoadFixtureSet(path)
	if err != nil {
		tb.Fatalf("LoadFixtureSet() error = %v", err)
	}
	return fixtures
}

func mustFixtureScenario(tb testing.TB, fixtures FixtureSet, name string) FixtureScenario {
	tb.Helper()

	scenario, ok := fixtures.Scenario(name)
	if !ok {
		tb.Fatalf("fixtures.Scenario(%q) not found", name)
	}
	return scenario
}

func mustRunPublishedScenario(tb testing.TB, harness *Harness, scenario FixtureScenario, label string) store.CycleRecord {
	tb.Helper()

	record, err := harness.RunPublishedCycle(PublishedCycleInput{
		RecordID: scenario.RecordID,
		At:       scenario.At,
		Quotes:   scenario.Quotes,
	})
	if err != nil {
		tb.Fatalf("RunPublishedCycle(%s) error = %v", label, err)
	}
	if record.PreparedPublication == nil {
		tb.Fatalf("%s prepared publication = nil, want value", label)
	}
	return record
}

func mustAckFixturePublication(tb testing.TB, harness *Harness, record store.CycleRecord, outcome FixtureSinkOutcome, label string) {
	tb.Helper()

	sequence := record.PreparedPublication.Sequence
	if err := harness.MarkPublicationSent(sequence, outcome.AckedAt); err != nil {
		tb.Fatalf("MarkPublicationSent(%s) error = %v", label, err)
	}
	if err := harness.AcknowledgePublication(sequence, outcome.AckedAt); err != nil {
		tb.Fatalf("AcknowledgePublication(%s) error = %v", label, err)
	}
}

func assertReplayedSequence(tb testing.TB, report operator.RecoveryReport, sequence uint64, label string) {
	tb.Helper()

	want := []uint64{sequence}
	if len(report.ReplayedSequences) != len(want) || report.ReplayedSequences[0] != sequence {
		tb.Fatalf("%s replayed sequences = %v, want %v", label, report.ReplayedSequences, want)
	}
}

type fixtureSink struct {
	kind     string
	outcomes []FixtureSinkOutcome
	calls    []store.PreparedPublication
}

func newFixtureSink(kind string, outcomes []FixtureSinkOutcome) *fixtureSink {
	return &fixtureSink{
		kind:     kind,
		outcomes: append([]FixtureSinkOutcome(nil), outcomes...),
	}
}

func (sink *fixtureSink) Kind() string {
	return sink.kind
}

func (sink *fixtureSink) Close() error {
	return nil
}

func (sink *fixtureSink) Publish(_ context.Context, publication store.PreparedPublication) (relayer.Ack, error) {
	sink.calls = append(sink.calls, publication)
	index := len(sink.calls) - 1
	if index >= len(sink.outcomes) {
		return relayer.Ack{}, errors.New("unexpected publish call")
	}
	return sink.outcomes[index].publishResult(publication)
}

func (sink *fixtureSink) publications() []store.PreparedPublication {
	return append([]store.PreparedPublication(nil), sink.calls...)
}

func (outcome FixtureSinkOutcome) publishResult(publication store.PreparedPublication) (relayer.Ack, error) {
	switch outcome.Kind {
	case "ack":
		return relayer.Ack{
			Sequence:       publication.Sequence,
			IdempotencyKey: publication.IdempotencyKey,
			PayloadHash:    publication.PayloadHash,
			Duplicate:      outcome.Duplicate,
			AckedAt:        outcome.AckedAt,
		}, nil
	case "retryable_timeout":
		return relayer.Ack{}, &relayer.PublishError{Retryable: true, Err: context.DeadlineExceeded}
	case "terminal_error":
		return relayer.Ack{}, errors.New("terminal publish failure")
	default:
		return relayer.Ack{}, errors.New("unsupported fixture sink outcome")
	}
}
