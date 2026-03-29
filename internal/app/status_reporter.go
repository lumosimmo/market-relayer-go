package app

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/api"
	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

type runtimeState struct {
	metadata api.Metadata
	clock    clock.Clock
	store    runtimeStatusStore
	markets  []config.MarketConfig
	loops    []*relayer.Loop

	mu              sync.RWMutex
	configLoaded    bool
	storeOpen       bool
	sinkInitialized bool
	loopStarted     bool
	loopLive        map[string]bool
	leases          map[string]leaseStatus
}

type leaseStatus struct {
	lease               store.Lease
	ownedByThisInstance bool
}

func newRuntimeState(
	metadata api.Metadata,
	serviceClock clock.Clock,
	cfg *config.Config,
	storeDB runtimeStatusStore,
	loops []*relayer.Loop,
) *runtimeState {
	loopLive := make(map[string]bool, len(loops))
	leases := make(map[string]leaseStatus, len(loops))
	for _, loop := range loops {
		loopLive[loop.Status().Market] = false
		leases[loop.Status().Market] = leaseStatus{}
	}

	return &runtimeState{
		metadata:        metadata,
		clock:           serviceClock,
		store:           storeDB,
		markets:         slices.Clone(cfg.Markets),
		loops:           append([]*relayer.Loop(nil), loops...),
		configLoaded:    true,
		storeOpen:       storeDB != nil,
		sinkInitialized: true,
		loopLive:        loopLive,
		leases:          leases,
	}
}

func (state *runtimeState) Health(ctx context.Context) api.HealthResponse {
	storeLive := state.storeLive(ctx)
	_, loopLive := state.loopHealth()

	return api.HealthResponse{
		Healthy:  storeLive && loopLive,
		Metadata: state.metadata,
		Checks: api.HealthChecks{
			StoreLive:           storeLive,
			LoopLive:            loopLive,
			LeaseControllerLive: loopLive,
		},
	}
}

func (state *runtimeState) Ready(context.Context) api.ReadyResponse {
	state.mu.RLock()
	defer state.mu.RUnlock()
	ready := state.configLoaded && state.storeOpen && state.sinkInitialized && state.loopStarted
	return api.ReadyResponse{
		Ready:           ready,
		ConfigLoaded:    state.configLoaded,
		StoreOpen:       state.storeOpen,
		SinkInitialized: state.sinkInitialized,
		LoopStarted:     state.loopStarted,
	}
}

func (state *runtimeState) Status(ctx context.Context) (api.StatusResponse, error) {
	now := state.now()
	loopStatus := state.loopSnapshots()

	response := api.StatusResponse{
		Metadata: state.metadata,
		Markets:  make([]api.MarketStatus, 0, len(state.markets)),
	}
	for _, market := range state.markets {
		snapshot, err := state.store.LatestSnapshot(ctx, market.Symbol)
		ownership := state.leaseSnapshot(market.Symbol)
		response.Markets = append(response.Markets, buildMarketStatus(now, snapshot, loopStatus[market.Symbol], ownership, err))
	}
	return response, nil
}

func (state *runtimeState) Snapshot(ctx context.Context, market string) (store.Snapshot, error) {
	selected, err := selectMarketFromList(state.markets, market)
	if err != nil {
		return store.Snapshot{}, err
	}
	snapshot, err := state.store.LatestSnapshot(ctx, selected.Symbol)
	if err != nil {
		return store.Snapshot{}, err
	}
	ownership := state.leaseSnapshot(selected.Symbol)
	snapshot.OwnedByThisInstance = ownership.ownedByThisInstance
	snapshot.LeaseOwner = ownership.lease.Owner
	if !ownership.lease.ExpiresAt.IsZero() {
		expires := ownership.lease.ExpiresAt
		snapshot.LeaseExpiresAt = &expires
	}
	return snapshot, nil
}

func (state *runtimeState) markLoopsStarted() {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.loopStarted = true
	for market := range state.loopLive {
		state.loopLive[market] = true
	}
}

func (state *runtimeState) markLoopStopped(market string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.loopLive == nil {
		return
	}
	if market != "" {
		state.loopLive[market] = false
		if state.leases != nil {
			state.leases[market] = leaseStatus{}
		}
	}
}

func (state *runtimeState) storeLive(ctx context.Context) bool {
	if state.store == nil {
		return false
	}
	err := state.store.Ping(ctx)
	return err == nil
}

func (state *runtimeState) loopHealth() (bool, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()

	if !state.loopStarted {
		return false, false
	}

	for _, live := range state.loopLive {
		if !live {
			return true, false
		}
	}
	return true, true
}

func (state *runtimeState) loopSnapshots() map[string]relayer.StatusSnapshot {
	snapshots := make(map[string]relayer.StatusSnapshot, len(state.loops))
	for _, loop := range state.loops {
		snapshot := loop.Status()
		snapshots[snapshot.Market] = snapshot
	}
	return snapshots
}

func (state *runtimeState) setLeaseStatus(market string, lease store.Lease, ownedByThisInstance bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.leases[market] = leaseStatus{
		lease:               lease,
		ownedByThisInstance: ownedByThisInstance,
	}
}

func (state *runtimeState) leaseSnapshot(market string) leaseStatus {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.leases[market]
}

func (state *runtimeState) now() time.Time {
	if state.clock == nil {
		return time.Now().UTC()
	}
	return state.clock.Now().UTC()
}

func buildMarketStatus(now time.Time, snapshot store.Snapshot, loop relayer.StatusSnapshot, ownership leaseStatus, storeErr error) api.MarketStatus {
	if errors.Is(storeErr, store.ErrSnapshotNotFound) {
		storeErr = nil
	}
	result := snapshot.Record.Computed
	publishability := effectivePublishability(loop.Publishability, snapshot.Publishability)
	marketStatus := api.MarketStatus{
		Market:              coalesceMarket(loop.Market, snapshot.Record.Market, snapshot.Publishability.Market),
		OwnedByThisInstance: ownership.ownedByThisInstance,
		LeaseOwner:          ownership.lease.Owner,
		Publishability:      publishability,
		SessionState:        coalesceSessionState(loop.SessionState, result.SessionState),
		SessionReason:       coalesceReason(loop.SessionReason, result.SessionReason),
		Prices: api.PriceSet{
			External:        api.PriceAvailability{Status: result.External.Status, Reason: result.External.Reason},
			Oracle:          api.PriceAvailability{Status: result.Oracle.Status, Reason: result.Oracle.Reason},
			OraclePlusBasis: api.PriceAvailability{Status: result.OraclePlusBasis.Status, Reason: result.OraclePlusBasis.Reason},
		},
	}
	if !ownership.lease.ExpiresAt.IsZero() {
		expires := ownership.lease.ExpiresAt
		marketStatus.LeaseExpiresAt = &expires
	}

	marketStatus.LastGoodAgeSeconds = lastGoodAgeSeconds(now, result.State.LastOracleAt)
	marketStatus.FallbackPublishable = fallbackStillPublishable(now, result.State, publishability.Status)
	marketStatus.State, marketStatus.Publishability = classifyMarketState(storeErr, marketStatus.Publishability, marketStatus.SessionState, marketStatus.Prices.Oracle, result.Oracle)
	return marketStatus
}

func coalesceMarket(values ...string) string {
	index := slices.IndexFunc(values, func(value string) bool {
		return strings.TrimSpace(value) != ""
	})
	if index < 0 {
		return ""
	}
	return values[index]
}

func effectivePublishability(loop status.Condition, stored store.PublishabilityState) status.Condition {
	if loop.Status != "" {
		return loop
	}
	if stored.Status != "" {
		return status.NewCondition(stored.Status, stored.Reason)
	}
	return status.NewCondition(status.StatusUnknown, status.ReasonNone)
}

func lastGoodAgeSeconds(now time.Time, lastGoodAt time.Time) *float64 {
	if lastGoodAt.IsZero() {
		return nil
	}

	age := now.Sub(lastGoodAt.UTC()).Seconds()
	if age < 0 {
		age = 0
	}
	return &age
}

func fallbackStillPublishable(now time.Time, state pricing.State, publishability status.Status) bool {
	return state.LastOracle != nil &&
		!state.OracleFallbackExpiresAt.IsZero() &&
		!now.After(state.OracleFallbackExpiresAt.UTC()) &&
		publishability != status.StatusUnavailable
}

func classifyMarketState(storeErr error, publishability status.Condition, sessionState status.SessionState, oracle api.PriceAvailability, computedOracle pricing.Price) (string, status.Condition) {
	switch {
	case storeErr != nil:
		return "unpublishable", status.NewCondition(status.StatusUnavailable, status.ReasonStoreUnavailable)
	case publishability.Status == status.StatusUnavailable:
		return "unpublishable", publishability
	case sessionState == status.SessionStateFallbackOnly || oracle.Reason == status.ReasonFallbackActive:
		return "fallback_only", publishability
	case !computedOracle.Available():
		return "unavailable", publishability
	default:
		return "fresh", publishability
	}
}

func coalesceSessionState(values ...status.SessionState) status.SessionState {
	if value := cmp.Or(values...); value != "" {
		return value
	}
	return status.SessionStateOpen
}

func coalesceReason(values ...status.Reason) status.Reason {
	if value := cmp.Or(values...); value != "" {
		return value
	}
	return status.ReasonNone
}

func selectMarketFromList(markets []config.MarketConfig, symbol string) (config.MarketConfig, error) {
	symbol = strings.TrimSpace(symbol)
	switch {
	case len(markets) == 0:
		return config.MarketConfig{}, fmt.Errorf("app: config does not define any markets")
	case symbol == "" && len(markets) == 1:
		return markets[0], nil
	case symbol == "":
		return config.MarketConfig{}, api.BadRequest(fmt.Errorf("app: market symbol is required when multiple markets are configured"))
	}

	index := slices.IndexFunc(markets, func(market config.MarketConfig) bool {
		return market.Symbol == symbol
	})
	if index >= 0 {
		return markets[index], nil
	}
	return config.MarketConfig{}, api.NotFound(fmt.Errorf("app: market %q not found in config", symbol))
}
