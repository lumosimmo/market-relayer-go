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

	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func TestHTTPSinkAcknowledgesCreatedAndDuplicateConflict(t *testing.T) {
	t.Parallel()

	publication := testPreparedPublication(7)

	tests := []struct {
		name       string
		statusCode int
		wantDup    bool
	}{
		{name: "created", statusCode: http.StatusCreated, wantDup: false},
		{name: "duplicate", statusCode: http.StatusConflict, wantDup: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if got := request.Header.Get("Idempotency-Key"); got != publication.IdempotencyKey {
					t.Fatalf("Idempotency-Key = %q, want %q", got, publication.IdempotencyKey)
				}
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(tt.statusCode)
				if err := json.NewEncoder(writer).Encode(Ack{
					Sequence:       publication.Sequence,
					IdempotencyKey: publication.IdempotencyKey,
					PayloadHash:    publication.PayloadHash,
					AckedAt:        time.Date(2026, 3, 27, 20, 0, 0, 0, time.UTC),
				}); err != nil {
					t.Fatalf("json.NewEncoder().Encode() error = %v", err)
				}
			}))
			defer server.Close()

			sink := &HTTPSink{
				client: &http.Client{Timeout: time.Second},
				url:    server.URL,
			}

			ack, err := sink.Publish(context.Background(), publication)
			if err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			if ack.Sequence != publication.Sequence {
				t.Fatalf("ack.Sequence = %d, want %d", ack.Sequence, publication.Sequence)
			}
			if ack.Duplicate != tt.wantDup {
				t.Fatalf("ack.Duplicate = %t, want %t", ack.Duplicate, tt.wantDup)
			}
		})
	}
}

func TestHTTPSinkTreatsServerErrorsAndTimeoutsAsRetryable(t *testing.T) {
	t.Parallel()

	publication := testPreparedPublication(8)

	serverErrServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "boom", http.StatusBadGateway)
	}))
	defer serverErrServer.Close()

	serverErrSink := &HTTPSink{
		client: &http.Client{Timeout: time.Second},
		url:    serverErrServer.URL,
	}

	_, err := serverErrSink.Publish(context.Background(), publication)
	if err == nil {
		t.Fatal("Publish() error = nil, want retryable publish error")
	}
	if !IsRetryablePublish(err) {
		t.Fatalf("Publish() error retryable = false, want true: %v", err)
	}

	timeoutServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		writer.WriteHeader(http.StatusCreated)
	}))
	defer timeoutServer.Close()

	timeoutSink := &HTTPSink{
		client: &http.Client{Timeout: 10 * time.Millisecond},
		url:    timeoutServer.URL,
	}
	_, err = timeoutSink.Publish(context.Background(), publication)
	if err == nil {
		t.Fatal("Publish() timeout error = nil, want retryable publish error")
	}
	if !IsRetryablePublish(err) {
		t.Fatalf("timeout Publish() error retryable = false, want true: %v", err)
	}
}

func TestHTTPSinkTreatsOtherClientErrorsAsTerminal(t *testing.T) {
	t.Parallel()

	publication := testPreparedPublication(9)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	sink := &HTTPSink{
		client: &http.Client{Timeout: time.Second},
		url:    server.URL,
	}

	_, err := sink.Publish(context.Background(), publication)
	if err == nil {
		t.Fatal("Publish() error = nil, want terminal publish error")
	}
	if IsRetryablePublish(err) {
		t.Fatalf("Publish() error retryable = true, want false: %v", err)
	}
}

func TestHTTPSinkAcceptsStatusOnlyAcknowledgements(t *testing.T) {
	t.Parallel()

	publication := testPreparedPublication(11)

	tests := []struct {
		name       string
		statusCode int
		wantDup    bool
	}{
		{name: "ok", statusCode: http.StatusOK, wantDup: false},
		{name: "created", statusCode: http.StatusCreated, wantDup: false},
		{name: "no content", statusCode: http.StatusNoContent, wantDup: false},
		{name: "conflict duplicate", statusCode: http.StatusConflict, wantDup: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if got := request.Header.Get("X-Payload-Hash"); got != publication.PayloadHash {
					t.Fatalf("X-Payload-Hash = %q, want %q", got, publication.PayloadHash)
				}
				writer.WriteHeader(tt.statusCode)
			}))
			defer server.Close()

			sink := &HTTPSink{
				client: &http.Client{Timeout: time.Second},
				url:    server.URL,
			}

			ack, err := sink.Publish(context.Background(), publication)
			if err != nil {
				t.Fatalf("Publish() error = %v, want nil", err)
			}
			if ack.Sequence != publication.Sequence {
				t.Fatalf("ack.Sequence = %d, want %d", ack.Sequence, publication.Sequence)
			}
			if ack.IdempotencyKey != publication.IdempotencyKey {
				t.Fatalf("ack.IdempotencyKey = %q, want %q", ack.IdempotencyKey, publication.IdempotencyKey)
			}
			if ack.PayloadHash != publication.PayloadHash {
				t.Fatalf("ack.PayloadHash = %q, want %q", ack.PayloadHash, publication.PayloadHash)
			}
			if ack.AckedAt.IsZero() {
				t.Fatal("ack.AckedAt is zero, want fallback timestamp")
			}
			if ack.Duplicate != tt.wantDup {
				t.Fatalf("ack.Duplicate = %t, want %t", ack.Duplicate, tt.wantDup)
			}
		})
	}
}

func TestHTTPSinkRejectsMalformedNonEmptyAcknowledgementBody(t *testing.T) {
	t.Parallel()

	publication := testPreparedPublication(12)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
		if _, err := writer.Write([]byte("not-json")); err != nil {
			t.Fatalf("writer.Write() error = %v", err)
		}
	}))
	defer server.Close()

	sink := &HTTPSink{
		client: &http.Client{Timeout: time.Second},
		url:    server.URL,
	}

	_, err := sink.Publish(context.Background(), publication)
	if err == nil {
		t.Fatal("Publish() error = nil, want malformed ack error")
	}
	if IsRetryablePublish(err) {
		t.Fatalf("Publish() error retryable = true, want false: %v", err)
	}
}

func TestFileSinkAppendsEnvelopes(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sink.jsonl")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	sink := &FileSink{file: file}
	t.Cleanup(func() {
		if closeErr := sink.Close(); closeErr != nil {
			t.Fatalf("sink.Close() error = %v", closeErr)
		}
	})

	publication := testPreparedPublication(10)
	ack, err := sink.Publish(context.Background(), publication)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if ack.Sequence != publication.Sequence {
		t.Fatalf("ack.Sequence = %d, want %d", ack.Sequence, publication.Sequence)
	}

	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile() error = %v", err)
	}
	if string(written) != string(append(append([]byte(nil), publication.Envelope...), '\n')) {
		t.Fatalf("written payload = %q, want %q", string(written), string(append(append([]byte(nil), publication.Envelope...), '\n')))
	}
}

func testPreparedPublication(sequence uint64) store.PreparedPublication {
	return store.PreparedPublication{
		Market:          "ETHUSD",
		Sequence:        sequence,
		State:           store.PublicationStatePrepared,
		PreparedAt:      time.Date(2026, 3, 27, 20, 0, 0, 0, time.UTC),
		IdempotencyKey:  "idempotency-key",
		PayloadHash:     "payload-hash",
		EnvelopeVersion: EnvelopeVersion,
		Envelope:        []byte(`{"payload":{"market":"ETHUSD"}}`),
	}
}
