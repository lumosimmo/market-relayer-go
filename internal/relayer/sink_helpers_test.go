package relayer

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

func TestPublishErrorFormattingAndUnwrap(t *testing.T) {
	t.Parallel()

	var nilErr *PublishError
	if got := nilErr.Error(); got != "<nil>" {
		t.Fatalf("(*PublishError)(nil).Error() = %q, want <nil>", got)
	}
	if nilErr.Unwrap() != nil {
		t.Fatal("(*PublishError)(nil).Unwrap() != nil")
	}

	baseErr := errors.New("boom")
	statusErr := &PublishError{StatusCode: 409, Err: baseErr}
	if got := statusErr.Error(); got != "relayer: publish status 409: boom" {
		t.Fatalf("statusErr.Error() = %q, want publish status text", got)
	}
	if !errors.Is(statusErr, baseErr) {
		t.Fatalf("errors.Is(statusErr, baseErr) = false, want true")
	}

	transportErr := &PublishError{Retryable: true, Err: baseErr}
	if got := transportErr.Error(); got != "relayer: publish error: boom" {
		t.Fatalf("transportErr.Error() = %q, want publish error text", got)
	}
	if !IsRetryablePublish(transportErr) {
		t.Fatal("IsRetryablePublish() = false, want true")
	}
	if IsRetryablePublish(baseErr) {
		t.Fatal("IsRetryablePublish(base error) = true, want false")
	}
}

func TestSinkConstructionAndKinds(t *testing.T) {
	t.Parallel()

	httpSink, err := NewSink(config.SinkConfig{
		Kind:           "http",
		URL:            "http://127.0.0.1:8080/publish",
		RequestTimeout: 250 * time.Millisecond,
		BearerToken:    "secret",
	})
	if err != nil {
		t.Fatalf("NewSink(http) error = %v", err)
	}
	httpTyped, ok := httpSink.(*HTTPSink)
	if !ok {
		t.Fatalf("NewSink(http) type = %T, want *HTTPSink", httpSink)
	}
	if got := httpTyped.Kind(); got != "http" {
		t.Fatalf("HTTPSink.Kind() = %q, want http", got)
	}
	if err := httpTyped.Close(); err != nil {
		t.Fatalf("HTTPSink.Close() error = %v, want nil", err)
	}

	filePath := filepath.Join(t.TempDir(), "sink.jsonl")
	fileSink, err := NewSink(config.SinkConfig{
		Kind: "file",
		Path: filePath,
	})
	if err != nil {
		t.Fatalf("NewSink(file) error = %v", err)
	}
	fileTyped, ok := fileSink.(*FileSink)
	if !ok {
		t.Fatalf("NewSink(file) type = %T, want *FileSink", fileSink)
	}
	if got := fileTyped.Kind(); got != "file" {
		t.Fatalf("FileSink.Kind() = %q, want file", got)
	}
	if err := fileTyped.Close(); err != nil {
		t.Fatalf("FileSink.Close() error = %v", err)
	}
	if err := (&FileSink{}).Close(); err != nil {
		t.Fatalf("nil FileSink.Close() error = %v, want nil", err)
	}

	_, err = NewSink(config.SinkConfig{Kind: "unknown"})
	if err == nil {
		t.Fatal("NewSink(unknown) error = nil, want unsupported sink kind")
	}
}
