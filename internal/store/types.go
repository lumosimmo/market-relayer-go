package store

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/status"
)

const SchemaVersion = "2"

var ErrMetadataMismatch = errors.New("store: exact verification metadata mismatch")
var ErrSnapshotNotFound = errors.New("store: latest snapshot not found")

type Metadata struct {
	ConfigDigest            string `json:"config_digest"`
	PricingAlgorithmVersion string `json:"pricing_algorithm_version"`
	SchemaVersion           string `json:"schema_version"`
	EnvelopeVersion         string `json:"envelope_version"`
}

type CycleRecord struct {
	RecordID            string               `json:"record_id"`
	Market              string               `json:"market"`
	RawQuotes           []feeds.Quote        `json:"raw_quotes"`
	RawBook             *feeds.BookSnapshot  `json:"raw_book,omitempty"`
	Computed            pricing.Result       `json:"computed"`
	Metadata            Metadata             `json:"metadata"`
	Publishability      PublishabilityState  `json:"publishability"`
	PreparedPublication *PreparedPublication `json:"prepared_publication,omitempty"`
}

type ExactQuery struct {
	Market   string
	RecordID string
	Metadata Metadata
}

type AuditResult struct {
	Record        CycleRecord `json:"record"`
	MetadataMatch bool        `json:"metadata_match"`
	Mismatches    []string    `json:"mismatches,omitempty"`
}

type Snapshot struct {
	SchemaVersion             string               `json:"schema_version"`
	OwnedByThisInstance       bool                 `json:"owned_by_this_instance,omitempty"`
	LeaseOwner                string               `json:"lease_owner,omitempty"`
	LeaseExpiresAt            *time.Time           `json:"lease_expires_at,omitempty"`
	Record                    CycleRecord          `json:"record"`
	Publishability            PublishabilityState  `json:"publishability"`
	LastSuccessfulPublication *PreparedPublication `json:"last_successful_publication,omitempty"`
	RecentCycles              []CycleRecord        `json:"recent_cycles,omitempty"`
}

type MetadataMismatchError struct {
	Fields []string
}

func (err *MetadataMismatchError) Error() string {
	return fmt.Sprintf("store: exact verification metadata mismatch: %s", strings.Join(err.Fields, ", "))
}

func (err *MetadataMismatchError) Unwrap() error {
	return ErrMetadataMismatch
}

type PublicationState string

const (
	PublicationStatePrepared PublicationState = "prepared"
	PublicationStateSent     PublicationState = "sent"
	PublicationStateAcked    PublicationState = "acked"
)

type PreparedPublication struct {
	Market          string           `json:"market"`
	Sequence        uint64           `json:"sequence"`
	State           PublicationState `json:"state"`
	PreparedAt      time.Time        `json:"prepared_at"`
	SentAt          *time.Time       `json:"sent_at,omitempty"`
	AckedAt         *time.Time       `json:"acked_at,omitempty"`
	IdempotencyKey  string           `json:"idempotency_key"`
	PayloadHash     string           `json:"payload_hash"`
	EnvelopeVersion string           `json:"envelope_version"`
	Envelope        []byte           `json:"envelope"`
}

type PublishabilityState struct {
	Market             string        `json:"market"`
	Status             status.Status `json:"status"`
	Reason             status.Reason `json:"reason"`
	UpdatedAt          time.Time     `json:"updated_at"`
	LastStableHash     string        `json:"last_stable_hash,omitempty"`
	LastSequence       uint64        `json:"last_sequence,omitempty"`
	LastIdempotencyKey string        `json:"last_idempotency_key,omitempty"`
	LastPayloadHash    string        `json:"last_payload_hash,omitempty"`
	PendingSequence    uint64        `json:"pending_sequence,omitempty"`
}
