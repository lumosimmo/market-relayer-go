package relayer

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/sessions"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

const defaultCadence = 3 * time.Second

type Store interface {
	CommitCycle(context.Context, store.CycleRecord) error
	PendingPublications(context.Context, string) ([]store.PreparedPublication, error)
	MarkPublicationSent(context.Context, string, uint64, time.Time) error
	AcknowledgePublication(context.Context, string, uint64, time.Time) error
	SafeSequence(context.Context, string) (uint64, error)
	LatestState(context.Context, string) (pricing.State, error)
	LatestPublishability(context.Context, string) (store.PublishabilityState, error)
	SavePublishability(context.Context, store.PublishabilityState) error
}

type Waiter interface {
	Wait(context.Context, time.Time) error
}

type realWaiter struct{}

func (realWaiter) Wait(ctx context.Context, until time.Time) error {
	delay := time.Until(until)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type PhaseBudgets struct {
	Fetch   time.Duration
	Compute time.Duration
	Persist time.Duration
	Publish time.Duration
}

func DefaultPhaseBudgets(interval time.Duration) PhaseBudgets {
	if interval <= 0 {
		interval = defaultCadence
	}
	return PhaseBudgets{
		Fetch:   scaleBudget(1800*time.Millisecond, interval),
		Compute: scaleBudget(250*time.Millisecond, interval),
		Persist: scaleBudget(200*time.Millisecond, interval),
		Publish: scaleBudget(500*time.Millisecond, interval),
	}
}

func scaleBudget(base time.Duration, interval time.Duration) time.Duration {
	if interval == defaultCadence {
		return base
	}
	scaled := time.Duration(int64(base) * int64(interval) / int64(defaultCadence))
	if scaled <= 0 {
		return time.Millisecond
	}
	return scaled
}

type AttemptBudgets struct {
	PublishAttemptTimeout time.Duration
}

type LoopOptions struct {
	Market         config.MarketConfig
	Metadata       store.Metadata
	Store          Store
	Sink           Sink
	Sources        []feeds.Source
	PerpBookSource feeds.BookSource
	Clock          clock.Clock
	Waiter         Waiter
	Logger         *slog.Logger
	Metrics        *metrics.Registry
	Window         sessions.WindowFunc
	Budgets        PhaseBudgets
	Attempts       AttemptBudgets
}

type StatusSnapshot struct {
	Market                string
	Publishability        status.Condition
	SessionState          status.SessionState
	SessionReason         status.Reason
	LastSequence          uint64
	PendingSequence       uint64
	LastIdempotencyKey    string
	LastPayloadHash       string
	LastCycleLatency      time.Duration
	LastPublishLatency    time.Duration
	LastPublicationResult string
}

type MetricsSnapshot struct {
	Cycles                 uint64
	PublicationsAcked      uint64
	RetryablePublishErrors uint64
	TerminalPublishErrors  uint64
	RecoveryReplays        uint64
	UnpublishableEvents    uint64
}

type Loop struct {
	market          config.MarketConfig
	metadata        store.Metadata
	store           Store
	sink            Sink
	sources         []feeds.Source
	perpBookSource  feeds.BookSource
	clock           clock.Clock
	waiter          Waiter
	logger          *slog.Logger
	metricsRegistry *metrics.Registry
	tracker         *sessions.Tracker
	window          sessions.WindowFunc
	budgets         PhaseBudgets
	attempts        AttemptBudgets

	runMu sync.Mutex
	mu    sync.RWMutex

	initialized           bool
	state                 pricing.State
	publishability        store.PublishabilityState
	nextSequence          uint64
	pending               *store.PreparedPublication
	cycleCounter          uint64
	blocked               bool
	blockedReason         status.Reason
	blockedAutoRecover    bool
	lastSessionState      status.SessionState
	lastSessionReason     status.Reason
	lastCycleLatency      time.Duration
	lastPublishLatency    time.Duration
	lastPublicationResult string
	metrics               MetricsSnapshot
}

func NewLoop(options LoopOptions) (*Loop, error) {
	if options.Store == nil {
		return nil, errors.New("relayer: store is required")
	}
	if options.Sink == nil {
		return nil, errors.New("relayer: sink is required")
	}
	if len(options.Sources) == 0 {
		return nil, errors.New("relayer: at least one source is required")
	}
	if options.Clock == nil {
		options.Clock = clock.Real{}
	}
	if options.Waiter == nil {
		options.Waiter = realWaiter{}
	}
	if options.Budgets.Fetch <= 0 || options.Budgets.Compute <= 0 || options.Budgets.Persist <= 0 || options.Budgets.Publish <= 0 {
		options.Budgets = DefaultPhaseBudgets(options.Market.PublishInterval)
	}
	if options.Attempts.PublishAttemptTimeout <= 0 {
		options.Attempts.PublishAttemptTimeout = minDuration(options.Budgets.Publish, time.Second)
	}

	tracker, err := sessions.NewTracker(options.Market, sessions.TrackerOptions{
		Logger: options.Logger,
		Window: options.Window,
	})
	if err != nil {
		return nil, err
	}

	return &Loop{
		market:          options.Market,
		metadata:        options.Metadata,
		store:           options.Store,
		sink:            options.Sink,
		sources:         slices.Clone(options.Sources),
		perpBookSource:  options.PerpBookSource,
		clock:           options.Clock,
		waiter:          options.Waiter,
		logger:          options.Logger,
		metricsRegistry: options.Metrics,
		tracker:         tracker,
		window:          options.Window,
		budgets:         options.Budgets,
		attempts:        options.Attempts,
		nextSequence:    1,
		publishability:  defaultPublishability(options.Market.Symbol, options.Clock.Now()),
	}, nil
}

func (loop *Loop) Run(ctx context.Context) error {
	if err := loop.initialize(ctx); err != nil {
		return err
	}

	nextTick := loop.clock.Now().UTC()
	for {
		now := loop.clock.Now().UTC()
		if now.Before(nextTick) {
			if err := loop.waiter.Wait(ctx, nextTick); err != nil {
				return err
			}
		}

		if err := loop.RunCycle(ctx); err != nil {
			return err
		}

		nextTick = nextTick.Add(loop.market.PublishInterval)
		now = loop.clock.Now().UTC()
		if now.After(nextTick) {
			if loop.metricsRegistry != nil {
				loop.metricsRegistry.IncTickOverrun(loop.market.Symbol)
			}
			missed := now.Sub(nextTick)/loop.market.PublishInterval + 1
			nextTick = nextTick.Add(missed * loop.market.PublishInterval)
		}
	}
}

func (loop *Loop) Status() StatusSnapshot {
	loop.mu.RLock()
	defer loop.mu.RUnlock()

	snapshot := StatusSnapshot{
		Market:                loop.market.Symbol,
		Publishability:        status.NewCondition(loop.publishability.Status, loop.publishability.Reason),
		SessionState:          loop.lastSessionState,
		SessionReason:         loop.lastSessionReason,
		LastSequence:          loop.publishability.LastSequence,
		LastIdempotencyKey:    loop.publishability.LastIdempotencyKey,
		LastPayloadHash:       loop.publishability.LastPayloadHash,
		LastCycleLatency:      loop.lastCycleLatency,
		LastPublishLatency:    loop.lastPublishLatency,
		LastPublicationResult: loop.lastPublicationResult,
	}
	if loop.pending != nil {
		snapshot.PendingSequence = loop.pending.Sequence
	}
	return snapshot
}

func (loop *Loop) Metrics() MetricsSnapshot {
	loop.mu.RLock()
	defer loop.mu.RUnlock()
	return loop.metrics
}

func (loop *Loop) Reset() {
	loop.runMu.Lock()
	defer loop.runMu.Unlock()

	loop.mu.Lock()
	defer loop.mu.Unlock()

	loop.initialized = false
	loop.state = pricing.State{}
	loop.publishability = defaultPublishability(loop.market.Symbol, loop.clock.Now())
	loop.nextSequence = 1
	loop.pending = nil
	loop.blocked = false
	loop.blockedReason = status.ReasonNone
	loop.blockedAutoRecover = false
	loop.lastSessionState = status.SessionState("")
	loop.lastSessionReason = status.ReasonNone
	loop.lastCycleLatency = 0
	loop.lastPublishLatency = 0
	loop.lastPublicationResult = ""
}

func (loop *Loop) fetchPhase(ctx context.Context) ([]feeds.Quote, *feeds.BookSnapshot, []error) {
	phaseCtx, cancel := context.WithTimeout(ctx, loop.budgets.Fetch)
	defer cancel()

	deadline := loop.clock.Now().UTC().Add(loop.budgets.Fetch)
	budget := feeds.FetchBudget{Deadline: deadline}

	results := feeds.FetchAll(phaseCtx, loop.sources, budget)
	bookCh := make(chan feeds.BookFetchResult, 1)
	go func() {
		bookCh <- feeds.FetchBook(phaseCtx, loop.perpBookSource, budget)
	}()

	quotes := make([]feeds.Quote, 0, len(loop.sources))
	errs := make([]error, 0)
	for _, current := range results {
		if current.Err != nil {
			errs = append(errs, current.Err)
			continue
		}
		quotes = append(quotes, current.Quote)
	}
	var book *feeds.BookSnapshot
	bookResult := <-bookCh
	if bookResult.Err != nil {
		errs = append(errs, bookResult.Err)
	} else if bookResult.Source != "" {
		copied := bookResult.Book
		book = &copied
	}
	return quotes, book, errs
}

func (loop *Loop) pendingPublication() *store.PreparedPublication {
	loop.mu.RLock()
	defer loop.mu.RUnlock()
	if loop.pending == nil {
		return nil
	}
	copied := *loop.pending
	return &copied
}

func (loop *Loop) shouldPreparePublication(pending *store.PreparedPublication) bool {
	if pending != nil {
		return false
	}

	loop.mu.RLock()
	defer loop.mu.RUnlock()
	return !loop.blocked
}

func (loop *Loop) logFetchResults(recordID string, errs []error) {
	if loop.logger == nil {
		return
	}
	for _, err := range errs {
		loop.logger.Warn(
			"source fetch rejected",
			"market", loop.market.Symbol,
			"cycle_id", recordID,
			"phase", "fetch",
			"error", err,
		)
	}
}

func (loop *Loop) logPublication(message string, recordID string, publication store.PreparedPublication, latency time.Duration, duplicate bool) {
	if loop.logger == nil {
		return
	}
	loop.logger.Info(
		message,
		"market", publication.Market,
		"cycle_id", recordID,
		"phase", "publish",
		"sequence", publication.Sequence,
		"idempotency_key", publication.IdempotencyKey,
		"payload_hash", publication.PayloadHash,
		"publish_latency", latency,
		"duplicate_ack", duplicate,
	)
}

func (loop *Loop) logPhase(message string, recordID string, phase string, attrs ...any) {
	if loop.logger == nil {
		return
	}
	base := []any{
		"market", loop.market.Symbol,
		"cycle_id", recordID,
		"phase", phase,
	}
	loop.logger.Info(message, append(base, attrs...)...)
}

func (loop *Loop) logError(message string, err error, recordID string, sequence uint64, idempotencyKey string) {
	if loop.logger == nil {
		return
	}
	loop.logger.Error(
		message,
		"market", loop.market.Symbol,
		"cycle_id", recordID,
		"phase", "publish",
		"sequence", sequence,
		"idempotency_key", idempotencyKey,
		"error", err,
	)
}

func minDuration(left time.Duration, right time.Duration) time.Duration {
	if left <= 0 {
		return right
	}
	if right <= 0 {
		return left
	}
	if left <= right {
		return left
	}
	return right
}
