package app

import (
	"errors"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/api"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func TestBuildMarketStatusTruthTable(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 14, 0, 0, 0, time.UTC)
	lastGood := now.Add(-5 * time.Second)

	tests := []struct {
		name      string
		snapshot  store.Snapshot
		loop      relayer.StatusSnapshot
		storeErr  error
		wantState string
		wantPrice status.Reason
		wantLast  bool
	}{
		{
			name:      "no snapshot yet",
			loop:      relayer.StatusSnapshot{Market: "ETHUSD", Publishability: status.NewCondition(status.StatusAvailable, status.ReasonNone)},
			storeErr:  store.ErrSnapshotNotFound,
			wantState: "unavailable",
			wantPrice: "",
			wantLast:  false,
		},
		{
			name: "fresh",
			snapshot: statusSnapshot(now, pricing.Result{
				SessionState:    status.SessionStateOpen,
				SessionReason:   status.ReasonNone,
				External:        price(status.StatusAvailable, status.ReasonNone, "1981.75"),
				Oracle:          price(status.StatusAvailable, status.ReasonNone, "1981.75"),
				OraclePlusBasis: price(status.StatusAvailable, status.ReasonNone, "1981.75"),
				State: pricing.State{
					LastOracleAt:            lastGood,
					OracleFallbackExpiresAt: now.Add(20 * time.Second),
				},
			}, status.NewCondition(status.StatusAvailable, status.ReasonNone)),
			loop:      relayer.StatusSnapshot{Publishability: status.NewCondition(status.StatusAvailable, status.ReasonNone), SessionState: status.SessionStateOpen, SessionReason: status.ReasonNone},
			wantState: "fresh",
			wantPrice: status.ReasonNone,
			wantLast:  true,
		},
		{
			name: "fallback only",
			snapshot: statusSnapshot(now, pricing.Result{
				SessionState:    status.SessionStateFallbackOnly,
				SessionReason:   status.ReasonFallbackActive,
				External:        price(status.StatusUnavailable, status.ReasonSourceRejected, ""),
				Oracle:          price(status.StatusDegraded, status.ReasonFallbackActive, "1981.75"),
				OraclePlusBasis: price(status.StatusDegraded, status.ReasonFallbackActive, "1981.75"),
				State: pricing.State{
					LastOracleAt:            lastGood,
					OracleFallbackExpiresAt: now.Add(20 * time.Second),
				},
			}, status.NewCondition(status.StatusAvailable, status.ReasonNone)),
			loop:      relayer.StatusSnapshot{Publishability: status.NewCondition(status.StatusAvailable, status.ReasonNone), SessionState: status.SessionStateFallbackOnly, SessionReason: status.ReasonFallbackActive},
			wantState: "fallback_only",
			wantPrice: status.ReasonFallbackActive,
			wantLast:  true,
		},
		{
			name: "unavailable with no last good",
			snapshot: statusSnapshot(now, pricing.Result{
				SessionState:    status.SessionStateExternalStale,
				SessionReason:   status.ReasonSourceStale,
				External:        price(status.StatusUnavailable, status.ReasonSourceStale, ""),
				Oracle:          price(status.StatusUnavailable, status.ReasonSourceStale, ""),
				OraclePlusBasis: price(status.StatusUnavailable, status.ReasonColdStart, ""),
			}, status.NewCondition(status.StatusAvailable, status.ReasonNone)),
			loop:      relayer.StatusSnapshot{Publishability: status.NewCondition(status.StatusAvailable, status.ReasonNone), SessionState: status.SessionStateExternalStale, SessionReason: status.ReasonSourceStale},
			wantState: "unavailable",
			wantPrice: status.ReasonSourceStale,
			wantLast:  false,
		},
		{
			name: "sink down",
			snapshot: statusSnapshot(now, pricing.Result{
				SessionState:    status.SessionStateOpen,
				SessionReason:   status.ReasonNone,
				External:        price(status.StatusAvailable, status.ReasonNone, "1981.75"),
				Oracle:          price(status.StatusAvailable, status.ReasonNone, "1981.75"),
				OraclePlusBasis: price(status.StatusAvailable, status.ReasonNone, "1981.75"),
				State: pricing.State{
					LastOracleAt:            lastGood,
					OracleFallbackExpiresAt: now.Add(20 * time.Second),
				},
			}, status.NewCondition(status.StatusUnavailable, status.ReasonSinkUnavailable)),
			loop:      relayer.StatusSnapshot{Publishability: status.NewCondition(status.StatusUnavailable, status.ReasonSinkUnavailable)},
			wantState: "unpublishable",
			wantPrice: status.ReasonNone,
			wantLast:  true,
		},
		{
			name: "store down",
			snapshot: statusSnapshot(now, pricing.Result{
				SessionState:    status.SessionStateOpen,
				SessionReason:   status.ReasonNone,
				External:        price(status.StatusAvailable, status.ReasonNone, "1981.75"),
				Oracle:          price(status.StatusAvailable, status.ReasonNone, "1981.75"),
				OraclePlusBasis: price(status.StatusAvailable, status.ReasonNone, "1981.75"),
			}, status.NewCondition(status.StatusAvailable, status.ReasonNone)),
			storeErr:  errors.New("store unavailable"),
			wantState: "unpublishable",
			wantPrice: status.ReasonNone,
			wantLast:  false,
		},
		{
			name: "durability unpublishable",
			snapshot: statusSnapshot(now, pricing.Result{
				SessionState:    status.SessionStateOpen,
				SessionReason:   status.ReasonNone,
				External:        price(status.StatusAvailable, status.ReasonNone, "1981.75"),
				Oracle:          price(status.StatusAvailable, status.ReasonNone, "1981.75"),
				OraclePlusBasis: price(status.StatusAvailable, status.ReasonNone, "1981.75"),
			}, status.NewCondition(status.StatusUnavailable, status.ReasonUnpublishable)),
			loop:      relayer.StatusSnapshot{Publishability: status.NewCondition(status.StatusUnavailable, status.ReasonUnpublishable)},
			wantState: "unpublishable",
			wantPrice: status.ReasonNone,
			wantLast:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := buildMarketStatus(now, tt.snapshot, tt.loop, leaseStatus{}, tt.storeErr)
			if got.State != tt.wantState {
				t.Fatalf("State = %q, want %q", got.State, tt.wantState)
			}
			if got.Prices.Oracle.Reason != tt.wantPrice {
				t.Fatalf("Oracle.Reason = %q, want %q", got.Prices.Oracle.Reason, tt.wantPrice)
			}
			if (got.LastGoodAgeSeconds != nil) != tt.wantLast {
				t.Fatalf("LastGoodAgeSeconds present = %v, want %v", got.LastGoodAgeSeconds != nil, tt.wantLast)
			}
		})
	}
}

func statusSnapshot(now time.Time, result pricing.Result, publishability status.Condition) store.Snapshot {
	return store.Snapshot{
		SchemaVersion: store.SchemaVersion,
		Record: store.CycleRecord{
			RecordID: "cycle-0001",
			Market:   "ETHUSD",
			Computed: result,
		},
		Publishability: store.PublishabilityState{
			Market:    "ETHUSD",
			Status:    publishability.Status,
			Reason:    publishability.Reason,
			UpdatedAt: now,
		},
	}
}

func price(state status.Status, reason status.Reason, raw string) pricing.Price {
	price := pricing.Price{
		Status: state,
		Reason: reason,
		Scale:  2,
	}
	if raw == "" {
		return price
	}

	value, err := fixedpoint.ParseDecimal(raw)
	if err != nil {
		panic(err)
	}
	price.Value = &value
	return price
}

var _ api.MarketStatus
