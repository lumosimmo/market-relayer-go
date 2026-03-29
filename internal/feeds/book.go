package feeds

import (
	"fmt"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
)

type BookLevel struct {
	Price fixedpoint.Value `json:"price"`
	Size  fixedpoint.Value `json:"size"`
}

type BookSnapshot struct {
	Source             string             `json:"source"`
	Symbol             string             `json:"symbol"`
	Market             string             `json:"market"`
	Bids               []BookLevel        `json:"bids"`
	Asks               []BookLevel        `json:"asks"`
	ReceivedAt         time.Time          `json:"received_at"`
	SourceTS           *time.Time         `json:"source_ts,omitempty"`
	FreshnessBasisKind FreshnessBasisKind `json:"freshness_basis_kind"`
	FreshnessBasisAt   time.Time          `json:"freshness_basis_at"`
}

func (book BookSnapshot) StableKey() string {
	bid := ""
	if len(book.Bids) > 0 {
		bid = fmt.Sprintf("%d:%d:%d:%d", book.Bids[0].Price.Int, book.Bids[0].Price.Scale, book.Bids[0].Size.Int, book.Bids[0].Size.Scale)
	}
	ask := ""
	if len(book.Asks) > 0 {
		ask = fmt.Sprintf("%d:%d:%d:%d", book.Asks[0].Price.Int, book.Asks[0].Price.Scale, book.Asks[0].Size.Int, book.Asks[0].Size.Scale)
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s", book.Source, book.Symbol, book.Market, bid, ask)
}
