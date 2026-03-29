package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/status"
)

func TestFormatPricePayload(t *testing.T) {
	t.Parallel()

	value := int64(9551579)
	if got, want := formatPricePayload(relayer.PricePayload{Status: status.StatusAvailable, Value: &value, Scale: 5}), "95.51579"; got != want {
		t.Fatalf("formatPricePayload(available) = %q, want %q", got, want)
	}
	if got, want := formatPricePayload(relayer.PricePayload{Status: status.StatusUnavailable, Reason: status.ReasonSessionClosed}), "unavailable(session_closed)"; got != want {
		t.Fatalf("formatPricePayload(unavailable) = %q, want %q", got, want)
	}
}

func TestPrinterServeHTTP(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 14, 30, 0, 0, time.UTC)
	var output bytes.Buffer
	printer := &printer{
		output: &output,
		now: func() time.Time {
			return now
		},
		seen: make(map[string]relayer.Ack),
	}

	value := int64(9551579)
	envelope := relayer.Envelope{
		Market:         "WTIUSD",
		Sequence:       7,
		IdempotencyKey: "key-1",
		PayloadHash:    "hash-1",
		Payload: relayer.Payload{
			Session: relayer.SessionPayload{
				State:  status.SessionStateOpen,
				Reason: status.ReasonNone,
			},
			Health: relayer.HealthPayload{
				Publishability: status.NewCondition(status.StatusAvailable, status.ReasonNone),
			},
			Selection: relayer.SelectionPayload{
				Rule:            "single_source_mid",
				SelectedSources: []string{"pyth-wti"},
			},
			External:        relayer.PricePayload{Status: status.StatusAvailable, Value: &value, Scale: 5},
			Oracle:          relayer.PricePayload{Status: status.StatusAvailable, Value: &value, Scale: 5},
			OraclePlusBasis: relayer.PricePayload{Status: status.StatusAvailable, Value: &value, Scale: 5},
		},
	}

	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(body))
	response := httptest.NewRecorder()
	printer.ServeHTTP(response, request)

	if got, want := response.Code, http.StatusOK; got != want {
		t.Fatalf("first response code = %d, want %d", got, want)
	}

	var ack relayer.Ack
	if err := json.Unmarshal(response.Body.Bytes(), &ack); err != nil {
		t.Fatalf("json.Unmarshal(first ack) error = %v", err)
	}
	if ack.Sequence != envelope.Sequence || ack.IdempotencyKey != envelope.IdempotencyKey || ack.PayloadHash != envelope.PayloadHash {
		t.Fatalf("first ack = %+v, want sequence/key/hash from envelope", ack)
	}

	secondRequest := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(body))
	secondResponse := httptest.NewRecorder()
	printer.ServeHTTP(secondResponse, secondRequest)

	if got, want := secondResponse.Code, http.StatusConflict; got != want {
		t.Fatalf("second response code = %d, want %d", got, want)
	}

	logged := output.String()
	if !strings.Contains(logged, "market=WTIUSD") || !strings.Contains(logged, "external=95.51579") {
		t.Fatalf("output = %q, want summary fields", logged)
	}
}
