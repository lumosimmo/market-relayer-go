package relayer

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

func TestSinkConstructorAndAckReadEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("file sink constructor returns open error", func(t *testing.T) {
		t.Parallel()

		_, err := NewSink(config.SinkConfig{
			Kind: "file",
			Path: t.TempDir(),
		})
		if err == nil {
			t.Fatal("NewSink(file directory) error = nil, want open failure")
		}
	})

	t.Run("decode ack returns terminal error when body cannot be read", func(t *testing.T) {
		t.Parallel()

		_, err := decodeAck(&http.Response{
			StatusCode: http.StatusCreated,
			Body:       errReadCloser{err: errors.New("read failed")},
		}, testPreparedPublication(35))
		if err == nil {
			t.Fatal("decodeAck(read failure) error = nil, want failure")
		}
		if IsRetryablePublish(err) {
			t.Fatalf("decodeAck(read failure) retryable = true, want false: %v", err)
		}
	})
}

type errReadCloser struct {
	err error
}

func (reader errReadCloser) Read([]byte) (int, error) {
	return 0, reader.err
}

func (errReadCloser) Close() error {
	return nil
}

var _ io.ReadCloser = errReadCloser{}
