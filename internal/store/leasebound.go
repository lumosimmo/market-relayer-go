package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/pricing"
)

type leaseBoundStore interface {
	VerifyExact(context.Context, ExactQuery) (CycleRecord, error)
	Audit(context.Context, ExactQuery) (AuditResult, error)
	PreviousCycle(context.Context, string, string) (CycleRecord, bool, error)
	PendingPublications(context.Context, string) ([]PreparedPublication, error)
	SafeSequence(context.Context, string) (uint64, error)
	LatestState(context.Context, string) (pricing.State, error)
	LatestPublishability(context.Context, string) (PublishabilityState, error)
	CommitCycleWithLease(context.Context, Lease, CycleRecord) error
	MarkPublicationSentWithLease(context.Context, Lease, string, uint64, time.Time) error
	AcknowledgePublicationWithLease(context.Context, Lease, string, uint64, time.Time) error
	SavePublishabilityWithLease(context.Context, Lease, PublishabilityState) error
}

type LeaseBoundRuntimeStore struct {
	db leaseBoundStore

	mu    sync.RWMutex
	lease Lease
}

func NewLeaseBoundRuntimeStore(db leaseBoundStore) *LeaseBoundRuntimeStore {
	return &LeaseBoundRuntimeStore{db: db}
}

func (storeDB *LeaseBoundRuntimeStore) UpdateLease(lease Lease) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.lease = lease
}

func (storeDB *LeaseBoundRuntimeStore) ClearLease() {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.lease = Lease{}
}

func (storeDB *LeaseBoundRuntimeStore) CurrentLease() (Lease, error) {
	storeDB.mu.RLock()
	defer storeDB.mu.RUnlock()
	if storeDB.lease == (Lease{}) {
		return Lease{}, errors.New("store: lease is not held")
	}
	return storeDB.lease, nil
}

func (storeDB *LeaseBoundRuntimeStore) CommitCycle(ctx context.Context, record CycleRecord) error {
	lease, err := storeDB.CurrentLease()
	if err != nil {
		return err
	}
	return storeDB.db.CommitCycleWithLease(ctx, lease, record)
}

func (storeDB *LeaseBoundRuntimeStore) PendingPublications(ctx context.Context, market string) ([]PreparedPublication, error) {
	return storeDB.db.PendingPublications(ctx, market)
}

func (storeDB *LeaseBoundRuntimeStore) VerifyExact(ctx context.Context, query ExactQuery) (CycleRecord, error) {
	return storeDB.db.VerifyExact(ctx, query)
}

func (storeDB *LeaseBoundRuntimeStore) Audit(ctx context.Context, query ExactQuery) (AuditResult, error) {
	return storeDB.db.Audit(ctx, query)
}

func (storeDB *LeaseBoundRuntimeStore) PreviousCycle(ctx context.Context, market string, recordID string) (CycleRecord, bool, error) {
	return storeDB.db.PreviousCycle(ctx, market, recordID)
}

func (storeDB *LeaseBoundRuntimeStore) MarkPublicationSent(ctx context.Context, market string, sequence uint64, sentAt time.Time) error {
	lease, err := storeDB.CurrentLease()
	if err != nil {
		return err
	}
	return storeDB.db.MarkPublicationSentWithLease(ctx, lease, market, sequence, sentAt)
}

func (storeDB *LeaseBoundRuntimeStore) AcknowledgePublication(ctx context.Context, market string, sequence uint64, ackedAt time.Time) error {
	lease, err := storeDB.CurrentLease()
	if err != nil {
		return err
	}
	return storeDB.db.AcknowledgePublicationWithLease(ctx, lease, market, sequence, ackedAt)
}

func (storeDB *LeaseBoundRuntimeStore) SafeSequence(ctx context.Context, market string) (uint64, error) {
	return storeDB.db.SafeSequence(ctx, market)
}

func (storeDB *LeaseBoundRuntimeStore) LatestState(ctx context.Context, market string) (pricing.State, error) {
	return storeDB.db.LatestState(ctx, market)
}

func (storeDB *LeaseBoundRuntimeStore) LatestPublishability(ctx context.Context, market string) (PublishabilityState, error) {
	return storeDB.db.LatestPublishability(ctx, market)
}

func (storeDB *LeaseBoundRuntimeStore) SavePublishability(ctx context.Context, state PublishabilityState) error {
	lease, err := storeDB.CurrentLease()
	if err != nil {
		return err
	}
	return storeDB.db.SavePublishabilityWithLease(ctx, lease, state)
}
