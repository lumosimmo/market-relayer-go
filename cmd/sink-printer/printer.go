package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/relayer"
)

type printer struct {
	output io.Writer
	raw    bool
	now    func() time.Time

	mu   sync.Mutex
	seen map[string]relayer.Ack
}

func (printer *printer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var envelope relayer.Envelope
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, "read request body", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		http.Error(writer, "decode request body", http.StatusBadRequest)
		return
	}

	ack, duplicate := printer.record(envelope)
	printer.printEnvelope(envelope, body, duplicate)

	statusCode := http.StatusOK
	if duplicate {
		statusCode = http.StatusConflict
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_ = json.NewEncoder(writer).Encode(ack)
}

func (printer *printer) record(envelope relayer.Envelope) (relayer.Ack, bool) {
	printer.mu.Lock()
	defer printer.mu.Unlock()

	if ack, ok := printer.seen[envelope.IdempotencyKey]; ok {
		return ack, true
	}

	ack := relayer.Ack{
		Sequence:       envelope.Sequence,
		IdempotencyKey: envelope.IdempotencyKey,
		PayloadHash:    envelope.PayloadHash,
		AckedAt:        printer.now().UTC(),
	}
	printer.seen[envelope.IdempotencyKey] = ack
	return ack, false
}

func (printer *printer) printEnvelope(envelope relayer.Envelope, body []byte, duplicate bool) {
	printer.mu.Lock()
	defer printer.mu.Unlock()

	line := fmt.Sprintf(
		"%s market=%s seq=%d duplicate=%t publishability=%s/%s session=%s/%s external=%s oracle=%s oracle_plus_basis=%s rule=%s selected=%s",
		printer.now().UTC().Format(time.RFC3339),
		envelope.Market,
		envelope.Sequence,
		duplicate,
		envelope.Payload.Health.Publishability.Status,
		envelope.Payload.Health.Publishability.Reason,
		envelope.Payload.Session.State,
		envelope.Payload.Session.Reason,
		formatPricePayload(envelope.Payload.External),
		formatPricePayload(envelope.Payload.Oracle),
		formatPricePayload(envelope.Payload.OraclePlusBasis),
		envelope.Payload.Selection.Rule,
		formatSelectedSources(envelope.Payload.Selection.SelectedSources),
	)
	_, _ = fmt.Fprintln(printer.output, line)
	if printer.raw {
		_, _ = fmt.Fprintln(printer.output, string(body))
	}
}
