package app

import (
	"context"
	"io"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

type runtimeStatusStore interface {
	Ping(context.Context) error
	LatestSnapshot(context.Context, string) (store.Snapshot, error)
	RecentCycles(context.Context, string, int) ([]store.CycleRecord, error)
}

type leaseManager interface {
	TryAcquireLease(context.Context, string, string, time.Duration) (store.Lease, bool, error)
	RenewLease(context.Context, store.Lease, time.Duration) (store.Lease, bool, error)
	ReleaseLease(context.Context, store.Lease) error
	CurrentLease(context.Context, string) (store.Lease, bool, error)
}

type runtimeStore interface {
	io.Closer
	runtimeStatusStore
	VerifyExact(context.Context, store.ExactQuery) (store.CycleRecord, error)
	Audit(context.Context, store.ExactQuery) (store.AuditResult, error)
	PreviousCycle(context.Context, string, string) (store.CycleRecord, bool, error)
	PendingPublications(context.Context, string) ([]store.PreparedPublication, error)
	SafeSequence(context.Context, string) (uint64, error)
	LatestState(context.Context, string) (pricing.State, error)
	LatestPublishability(context.Context, string) (store.PublishabilityState, error)
	CommitCycleWithLease(context.Context, store.Lease, store.CycleRecord) error
	MarkPublicationSentWithLease(context.Context, store.Lease, string, uint64, time.Time) error
	AcknowledgePublicationWithLease(context.Context, store.Lease, string, uint64, time.Time) error
	SavePublishabilityWithLease(context.Context, store.Lease, store.PublishabilityState) error
	leaseManager
}

var _ runtimeStore = (*store.DB)(nil)
