package relayer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHTTPSinkNormalizesAcknowledgementTimeAndBearerToken(t *testing.T) {
	t.Parallel()

	publication := testPreparedPublication(13)
	ackedAt := time.Date(2026, 3, 27, 11, 0, 0, 0, time.FixedZone("CET", 3600))

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("Authorization = %q, want Bearer secret", got)
		}
		writer.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(writer).Encode(Ack{
			Sequence:       publication.Sequence,
			IdempotencyKey: publication.IdempotencyKey,
			PayloadHash:    publication.PayloadHash,
			AckedAt:        ackedAt,
		}); err != nil {
			t.Fatalf("json.NewEncoder().Encode() error = %v", err)
		}
	}))
	defer server.Close()

	sink := &HTTPSink{
		client:      &http.Client{Timeout: time.Second},
		url:         server.URL,
		bearerToken: "secret",
	}

	ack, err := sink.Publish(context.Background(), publication)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if !ack.AckedAt.Equal(ackedAt.UTC()) {
		t.Fatalf("ack.AckedAt = %s, want %s", ack.AckedAt, ackedAt.UTC())
	}
}

func TestHTTPSinkRejectsMismatchedAcknowledgementAndInvalidURL(t *testing.T) {
	t.Parallel()

	t.Run("mismatched ack is terminal", func(t *testing.T) {
		t.Parallel()

		publication := testPreparedPublication(14)
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusCreated)
			if err := json.NewEncoder(writer).Encode(Ack{
				Sequence:       publication.Sequence + 1,
				IdempotencyKey: publication.IdempotencyKey,
				PayloadHash:    publication.PayloadHash,
			}); err != nil {
				t.Fatalf("json.NewEncoder().Encode() error = %v", err)
			}
		}))
		defer server.Close()

		sink := &HTTPSink{
			client: &http.Client{Timeout: time.Second},
			url:    server.URL,
		}

		_, err := sink.Publish(context.Background(), publication)
		if err == nil {
			t.Fatal("Publish() error = nil, want ack mismatch")
		}
		if IsRetryablePublish(err) {
			t.Fatalf("Publish() error retryable = true, want false: %v", err)
		}
	})

	t.Run("invalid request url is terminal", func(t *testing.T) {
		t.Parallel()

		_, err := (&HTTPSink{
			client: &http.Client{Timeout: time.Second},
			url:    "://bad-url",
		}).Publish(context.Background(), testPreparedPublication(15))
		if err == nil {
			t.Fatal("Publish(invalid url) error = nil, want request construction failure")
		}
		if IsRetryablePublish(err) {
			t.Fatalf("Publish(invalid url) retryable = true, want false: %v", err)
		}
	})
}

func TestFileSinkReturnsErrorWhenFileIsClosed(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "closed-sink.jsonl")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	sink := &FileSink{file: file}
	if err := sink.Close(); err != nil {
		t.Fatalf("sink.Close() error = %v", err)
	}

	_, err = sink.Publish(context.Background(), testPreparedPublication(16))
	if err == nil {
		t.Fatal("Publish(closed file) error = nil, want write failure")
	}
	if IsRetryablePublish(err) {
		t.Fatalf("Publish(closed file) retryable = true, want false: %v", err)
	}
}
