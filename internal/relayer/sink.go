package relayer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

type Ack struct {
	Sequence       uint64    `json:"sequence,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	PayloadHash    string    `json:"payload_hash,omitempty"`
	AckedAt        time.Time `json:"acked_at,omitempty"`
	Duplicate      bool      `json:"duplicate,omitempty"`
}

type PublishError struct {
	Retryable  bool
	StatusCode int
	Err        error
}

func (err *PublishError) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.StatusCode != 0 {
		return fmt.Sprintf("relayer: publish status %d: %v", err.StatusCode, err.Err)
	}
	return fmt.Sprintf("relayer: publish error: %v", err.Err)
}

func (err *PublishError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

type Sink interface {
	Kind() string
	Publish(context.Context, store.PreparedPublication) (Ack, error)
	Close() error
}

type HTTPSink struct {
	client      *http.Client
	url         string
	bearerToken string
}

func (sink *HTTPSink) Kind() string {
	return "http"
}

func (sink *HTTPSink) Publish(ctx context.Context, publication store.PreparedPublication) (Ack, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, sink.url, bytes.NewReader(publication.Envelope))
	if err != nil {
		return Ack{}, &PublishError{Retryable: false, Err: err}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", publication.IdempotencyKey)
	request.Header.Set("X-Payload-Hash", publication.PayloadHash)
	if sink.bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+sink.bearerToken)
	}

	response, err := sink.client.Do(request)
	if err != nil {
		return Ack{}, publishTransportError(err)
	}
	defer response.Body.Close()

	switch {
	case response.StatusCode == http.StatusOK, response.StatusCode == http.StatusCreated, response.StatusCode == http.StatusNoContent, response.StatusCode == http.StatusConflict:
		ack, err := decodeAck(response, publication)
		if err != nil {
			return Ack{}, err
		}
		ack.Duplicate = response.StatusCode == http.StatusConflict
		return ack, nil
	case response.StatusCode >= http.StatusInternalServerError:
		return Ack{}, publishStatusError(response.StatusCode, true)
	default:
		return Ack{}, publishStatusError(response.StatusCode, false)
	}
}

func (sink *HTTPSink) Close() error {
	return nil
}

type FileSink struct {
	file *os.File
}

func (sink *FileSink) Kind() string {
	return "file"
}

func (sink *FileSink) Publish(_ context.Context, publication store.PreparedPublication) (Ack, error) {
	if _, err := sink.file.Write(append(append([]byte(nil), publication.Envelope...), '\n')); err != nil {
		return Ack{}, &PublishError{Retryable: false, Err: err}
	}
	return Ack{
		Sequence:       publication.Sequence,
		IdempotencyKey: publication.IdempotencyKey,
		PayloadHash:    publication.PayloadHash,
		AckedAt:        time.Now().UTC(),
	}, nil
}

func (sink *FileSink) Close() error {
	if sink.file == nil {
		return nil
	}
	return sink.file.Close()
}

func NewSink(cfg config.SinkConfig) (Sink, error) {
	switch cfg.Kind {
	case "http":
		return &HTTPSink{
			client:      &http.Client{Timeout: cfg.RequestTimeout},
			url:         cfg.URL,
			bearerToken: cfg.BearerToken,
		}, nil
	case "file":
		file, err := os.OpenFile(cfg.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		return &FileSink{file: file}, nil
	default:
		return nil, fmt.Errorf("relayer: unsupported sink kind %s", cfg.Kind)
	}
}

func decodeAck(response *http.Response, publication store.PreparedPublication) (Ack, error) {
	var ack Ack
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return Ack{}, &PublishError{Retryable: false, StatusCode: response.StatusCode, Err: fmt.Errorf("read ack body: %w", err)}
	}
	if trimmed := bytes.TrimSpace(body); len(trimmed) > 0 {
		if err := json.Unmarshal(trimmed, &ack); err != nil {
			return Ack{}, &PublishError{Retryable: false, StatusCode: response.StatusCode, Err: fmt.Errorf("decode ack body: %w", err)}
		}
	}

	ack, err = ValidateAck(ack, publication)
	if err != nil {
		var publishErr *PublishError
		if errors.As(err, &publishErr) && publishErr.StatusCode == 0 {
			publishErr.StatusCode = response.StatusCode
		}
		return Ack{}, err
	}
	return ack, nil
}

func ValidateAck(ack Ack, publication store.PreparedPublication) (Ack, error) {
	applyAckDefaults(&ack, publication)
	if !ackMatchesPublication(ack, publication) {
		return Ack{}, &PublishError{
			Retryable: false,
			Err:       fmt.Errorf("ack mismatch: sequence=%d key=%q hash=%q", ack.Sequence, ack.IdempotencyKey, ack.PayloadHash),
		}
	}
	if ack.AckedAt.IsZero() {
		ack.AckedAt = time.Now().UTC()
	} else {
		ack.AckedAt = ack.AckedAt.UTC()
	}
	return ack, nil
}

func IsRetryablePublish(err error) bool {
	var publishErr *PublishError
	return errors.As(err, &publishErr) && publishErr.Retryable
}

func publishTransportError(err error) error {
	retryable := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		retryable = true
	}
	return &PublishError{Retryable: retryable, Err: err}
}

func publishStatusError(statusCode int, retryable bool) error {
	return &PublishError{
		Retryable:  retryable,
		StatusCode: statusCode,
		Err:        fmt.Errorf("sink returned %d %s", statusCode, http.StatusText(statusCode)),
	}
}

func applyAckDefaults(ack *Ack, publication store.PreparedPublication) {
	if ack.IdempotencyKey == "" {
		ack.IdempotencyKey = publication.IdempotencyKey
	}
	if ack.PayloadHash == "" {
		ack.PayloadHash = publication.PayloadHash
	}
	if ack.Sequence == 0 {
		ack.Sequence = publication.Sequence
	}
}

func ackMatchesPublication(ack Ack, publication store.PreparedPublication) bool {
	return ack.IdempotencyKey == publication.IdempotencyKey &&
		ack.PayloadHash == publication.PayloadHash &&
		ack.Sequence == publication.Sequence
}
