package relayer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/sessions"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func (loop *Loop) RunCycle(ctx context.Context) error {
	if err := loop.initialize(ctx); err != nil {
		return err
	}

	loop.runMu.Lock()
	defer loop.runMu.Unlock()

	startedAt := loop.clock.Now().UTC()
	recordID := loop.nextRecordID(startedAt)

	fetchStarted := loop.clock.Now().UTC()
	quotes, book, fetchErrs := loop.fetchPhase(ctx)
	fetchLatency := loop.clock.Now().UTC().Sub(fetchStarted)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if loop.metricsRegistry != nil {
		loop.metricsRegistry.ObserveFetchLatency(loop.market.Symbol, fetchLatency)
	}
	loop.logFetchResults(recordID, fetchErrs)
	loop.observeFetchMetrics(startedAt, quotes, fetchErrs)
	loop.logPhase("fetch completed", recordID, "fetch", "healthy_quotes", len(quotes), "rejected_quotes", len(fetchErrs))

	record, prepared, computed, err := loop.prepareCycleRecord(ctx, recordID, startedAt, quotes, book)
	if err != nil {
		return err
	}

	persistStarted := loop.clock.Now().UTC()
	if err := loop.persistPhase(ctx, record); err != nil {
		loop.handleStoreFailure()
		loop.logError("persist failed; market is now unpublishable", err, recordID, 0, "")
		loop.updateCycleStats(computed, startedAt, 0, "persist_failed")
		return nil
	}
	if loop.metricsRegistry != nil {
		loop.metricsRegistry.ObservePersistLatency(loop.market.Symbol, loop.clock.Now().UTC().Sub(persistStarted))
	}
	loop.logPhase("persist completed", recordID, "persist", "prepared_publication", record.PreparedPublication != nil)

	if loop.applyPersistedCycle(record, prepared) {
		loop.clearBlocked(ctx, recordID)
		loop.updateCycleStats(computed, startedAt, 0, "durability_recovered")
		return nil
	}

	pending := loop.pendingPublication()
	if pending == nil {
		loop.updateCycleStats(computed, startedAt, 0, "skipped")
		return nil
	}

	publishLatency, publishResult, err := loop.publishPhase(ctx, *pending, recordID)
	if loop.metricsRegistry != nil {
		loop.metricsRegistry.ObservePublishLatency(loop.market.Symbol, publishLatency)
	}
	loop.updateCycleStats(computed, startedAt, publishLatency, publishResult)
	if err != nil {
		return err
	}
	return nil
}

func (loop *Loop) prepareCycleRecord(
	ctx context.Context,
	recordID string,
	startedAt time.Time,
	quotes []feeds.Quote,
	book *feeds.BookSnapshot,
) (store.CycleRecord, *store.PreparedPublication, pricing.Result, error) {
	priorState := loop.restoreStateAt(startedAt)
	computeStarted := loop.clock.Now().UTC()
	computed, err := loop.computePhase(ctx, priorState, quotes, book, recordID)
	if err != nil {
		return store.CycleRecord{}, nil, pricing.Result{}, err
	}
	if loop.metricsRegistry != nil {
		loop.metricsRegistry.ObserveComputeLatency(loop.market.Symbol, loop.clock.Now().UTC().Sub(computeStarted))
	}
	loop.observeClamp(priorState, computed)
	loop.logPhase("compute completed", recordID, "compute", "session_state", computed.SessionState, "session_reason", computed.SessionReason)

	record := store.CycleRecord{
		RecordID:  recordID,
		Market:    loop.market.Symbol,
		RawQuotes: quotes,
		RawBook:   cloneBookSnapshot(book),
		Computed:  computed,
		Metadata:  loop.metadata,
	}

	pending := loop.pendingPublication()
	record.Publishability = loop.publishabilityForCycle(startedAt, pending)

	if !loop.shouldPreparePublication(pending) {
		return record, nil, computed, nil
	}

	stableHash, publication, err := loop.prepareIfChanged(computed, record.Publishability, startedAt)
	if err != nil {
		return store.CycleRecord{}, nil, pricing.Result{}, err
	}
	if publication == nil {
		return record, nil, computed, nil
	}

	record.PreparedPublication = publication
	record.Publishability = publishabilityWithPrepared(record.Publishability, stableHash, *publication)
	return record, publication, computed, nil
}

func (loop *Loop) applyPersistedCycle(record store.CycleRecord, prepared *store.PreparedPublication) bool {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	previous := loop.publishability

	loop.state = record.Computed.State
	loop.lastSessionState = record.Computed.SessionState
	loop.lastSessionReason = record.Computed.SessionReason
	loop.publishability = record.Publishability

	if prepared != nil {
		copied := *prepared
		loop.pending = &copied
		loop.nextSequence = copied.Sequence + 1
	}
	loop.recordPublishabilityTransition(previous, loop.publishability)

	return loop.blocked && loop.blockedAutoRecover
}

func (loop *Loop) computePhase(ctx context.Context, prior pricing.State, quotes []feeds.Quote, book *feeds.BookSnapshot, recordID string) (pricing.Result, error) {
	if err := ctx.Err(); err != nil {
		return pricing.Result{}, err
	}

	computedAt := loop.clock.Now()
	window := sessions.OpenWindow()
	if loop.window != nil {
		window = loop.window(computedAt)
	}

	computed, err := pricing.Compute(pricing.Inputs{
		Market:         loop.market,
		Quotes:         quotes,
		Book:           cloneBookSnapshot(book),
		Prior:          prior,
		At:             computedAt,
		ExternalClosed: window.State == status.SessionStateExternalStale || window.State == status.SessionStateClosed,
	})
	if err != nil {
		return pricing.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return pricing.Result{}, err
	}

	return loop.tracker.Apply(computed, prior, sessions.CorrelationIDs{CycleID: recordID}), nil
}

func cloneBookSnapshot(book *feeds.BookSnapshot) *feeds.BookSnapshot {
	if book == nil {
		return nil
	}
	copied := *book
	copied.Bids = append([]feeds.BookLevel(nil), book.Bids...)
	copied.Asks = append([]feeds.BookLevel(nil), book.Asks...)
	if book.SourceTS != nil {
		ts := book.SourceTS.UTC()
		copied.SourceTS = &ts
	}
	return &copied
}

func (loop *Loop) prepareIfChanged(
	computed pricing.Result,
	publishability store.PublishabilityState,
	preparedAt time.Time,
) (string, *store.PreparedPublication, error) {
	stableHash, prepared, err := PreparePublication(loop.metadata, computed, publishability, loop.nextSequence, preparedAt)
	if err != nil {
		return "", nil, err
	}

	loop.mu.RLock()
	currentHash := loop.publishability.LastStableHash
	loop.mu.RUnlock()
	if currentHash == stableHash {
		return stableHash, nil, nil
	}
	return stableHash, &prepared, nil
}

func (loop *Loop) persistPhase(ctx context.Context, record store.CycleRecord) error {
	phaseCtx, cancel := context.WithTimeout(ctx, loop.budgets.Persist)
	defer cancel()
	if err := phaseCtx.Err(); err != nil {
		return err
	}
	if err := loop.store.CommitCycle(phaseCtx, record); err != nil {
		return err
	}
	return phaseCtx.Err()
}

func (loop *Loop) updateCycleStats(computed pricing.Result, startedAt time.Time, publishLatency time.Duration, result string) {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	loop.lastSessionState = computed.SessionState
	loop.lastSessionReason = computed.SessionReason
	loop.lastCycleLatency = loop.clock.Now().UTC().Sub(startedAt)
	loop.lastPublishLatency = publishLatency
	loop.lastPublicationResult = result
	loop.metrics.Cycles++
}

func (loop *Loop) nextRecordID(startedAt time.Time) string {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	loop.cycleCounter++
	return fmt.Sprintf("%s-%020d-%06d", loop.market.Symbol, startedAt.UnixNano(), loop.cycleCounter)
}

func (loop *Loop) observeFetchMetrics(observedAt time.Time, quotes []feeds.Quote, errs []error) {
	if loop.metricsRegistry == nil {
		return
	}
	for _, quote := range quotes {
		age := observedAt.Sub(quote.FreshnessBasisAt.UTC())
		if age < 0 {
			age = 0
		}
		loop.metricsRegistry.ObserveSourceFreshness(loop.market.Symbol, quote.Source, quote.FreshnessBasisKind, age)
	}
	for _, err := range errs {
		var fetchErr *feeds.Error
		if !errors.As(err, &fetchErr) {
			continue
		}
		loop.metricsRegistry.IncRejectedUpdate(loop.market.Symbol, fetchErr.Source, fetchErr.Reason)
		switch fetchErr.Kind {
		case feeds.ErrorFrozenQuote:
			loop.metricsRegistry.IncQuoteFreeze(loop.market.Symbol, fetchErr.Source)
		case feeds.ErrorRateLimited, feeds.ErrorCooldown:
			loop.metricsRegistry.IncRateLimitCooldown(loop.market.Symbol, fetchErr.Source)
		}
	}
}

func (loop *Loop) observeClamp(prior pricing.State, computed pricing.Result) {
	if loop.metricsRegistry == nil || prior.LastOracle == nil || computed.Oracle.Value == nil {
		return
	}
	if !computed.Oracle.Available() || computed.Oracle.Reason == status.ReasonFallbackActive {
		return
	}
	if computed.OracleClamped {
		loop.metricsRegistry.IncOracleClamp(loop.market.Symbol)
	}
}
