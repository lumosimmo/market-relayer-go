package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (db *DB) PendingPublications(ctx context.Context, market string) ([]PreparedPublication, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT market, sequence, state, prepared_at, sent_at, acked_at, idempotency_key, payload_hash, envelope_version, envelope
		FROM publications
		WHERE market = $1 AND state <> 'acked'
		ORDER BY sequence ASC
	`, strings.TrimSpace(market))
	if err != nil {
		return nil, fmt.Errorf("store: load pending publications: %w", err)
	}
	defer rows.Close()

	publications := make([]PreparedPublication, 0)
	for rows.Next() {
		publication, err := scanPublication(rows)
		if err != nil {
			return nil, err
		}
		publications = append(publications, publication)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: load pending publications: %w", err)
	}
	return publications, nil
}

func (db *DB) MarkPublicationSent(ctx context.Context, market string, sequence uint64, sentAt time.Time) error {
	return db.markPublicationSent(ctx, nil, market, sequence, sentAt)
}

func (db *DB) MarkPublicationSentWithLease(ctx context.Context, lease Lease, market string, sequence uint64, sentAt time.Time) error {
	return db.markPublicationSent(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return assertLease(ctx, tx, lease)
	}, market, sequence, sentAt)
}

func (db *DB) AcknowledgePublication(ctx context.Context, market string, sequence uint64, ackedAt time.Time) error {
	return db.acknowledgePublication(ctx, nil, market, sequence, ackedAt)
}

func (db *DB) AcknowledgePublicationWithLease(ctx context.Context, lease Lease, market string, sequence uint64, ackedAt time.Time) error {
	return db.acknowledgePublication(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return assertLease(ctx, tx, lease)
	}, market, sequence, ackedAt)
}

func (db *DB) markPublicationSent(ctx context.Context, guard leaseGuard, market string, sequence uint64, sentAt time.Time) error {
	return db.withTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if guard != nil {
			if err := guard(ctx, tx); err != nil {
				return err
			}
		}
		commandTag, err := tx.Exec(ctx, `
			UPDATE publications
			SET state = CASE WHEN state = 'acked' THEN state ELSE 'sent' END,
			    sent_at = CASE WHEN state = 'acked' THEN sent_at ELSE $3 END,
			    updated_at = $3
			WHERE market = $1 AND sequence = $2
		`, strings.TrimSpace(market), int64(sequence), sentAt.UTC())
		if err != nil {
			return fmt.Errorf("store: mark publication sent: %w", err)
		}
		if commandTag.RowsAffected() == 0 {
			return fmt.Errorf("store: prepared publication %s/%d not found", market, sequence)
		}
		return nil
	})
}

func (db *DB) acknowledgePublication(ctx context.Context, guard leaseGuard, market string, sequence uint64, ackedAt time.Time) error {
	return db.withTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if guard != nil {
			if err := guard(ctx, tx); err != nil {
				return err
			}
		}
		commandTag, err := tx.Exec(ctx, `
			UPDATE publications
			SET state = 'acked',
			    acked_at = $3,
			    updated_at = $3
			WHERE market = $1 AND sequence = $2
		`, strings.TrimSpace(market), int64(sequence), ackedAt.UTC())
		if err != nil {
			return fmt.Errorf("store: acknowledge publication: %w", err)
		}
		if commandTag.RowsAffected() == 0 {
			return fmt.Errorf("store: prepared publication %s/%d not found", market, sequence)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO market_runtime (market, safe_sequence, pending_sequence, updated_at)
			VALUES ($1, $2, 0, $3)
			ON CONFLICT (market) DO UPDATE SET
			    safe_sequence = GREATEST(market_runtime.safe_sequence, EXCLUDED.safe_sequence),
			    pending_sequence = CASE
			        WHEN market_runtime.pending_sequence = EXCLUDED.safe_sequence THEN 0
			        ELSE market_runtime.pending_sequence
			    END,
			    updated_at = EXCLUDED.updated_at
		`, strings.TrimSpace(market), int64(sequence), ackedAt.UTC()); err != nil {
			return fmt.Errorf("store: advance safe sequence: %w", err)
		}
		return nil
	})
}

func insertPublication(ctx context.Context, tx pgx.Tx, publication PreparedPublication) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO publications (
			market, sequence, state, prepared_at, sent_at, acked_at, idempotency_key, payload_hash, envelope_version, envelope, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, publication.Market, int64(publication.Sequence), string(publication.State), publication.PreparedAt.UTC(), publication.SentAt, publication.AckedAt, publication.IdempotencyKey, publication.PayloadHash, publication.EnvelopeVersion, publication.Envelope, publication.PreparedAt.UTC())
	if err != nil {
		return fmt.Errorf("store: insert publication: %w", err)
	}
	return nil
}

func scanPublication(rows pgx.Rows) (PreparedPublication, error) {
	var publication PreparedPublication
	var sequence int64
	var state string
	if err := rows.Scan(
		&publication.Market,
		&sequence,
		&state,
		&publication.PreparedAt,
		&publication.SentAt,
		&publication.AckedAt,
		&publication.IdempotencyKey,
		&publication.PayloadHash,
		&publication.EnvelopeVersion,
		&publication.Envelope,
	); err != nil {
		return PreparedPublication{}, fmt.Errorf("store: scan publication: %w", err)
	}
	publication.Sequence = uint64(sequence)
	publication.State = PublicationState(state)
	return publication, nil
}
