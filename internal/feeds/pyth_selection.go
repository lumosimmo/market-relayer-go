package feeds

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
)

var (
	pythExpiryPattern = regexp.MustCompile(`(\d{1,2})\s+([A-Z]+)\s+(\d{4})\s*/`)
	pythExpiryMonths  = map[string]time.Month{
		"JANUARY":   time.January,
		"FEBRUARY":  time.February,
		"MARCH":     time.March,
		"APRIL":     time.April,
		"MAY":       time.May,
		"JUNE":      time.June,
		"JULY":      time.July,
		"AUGUST":    time.August,
		"SEPTEMBER": time.September,
		"OCTOBER":   time.October,
		"NOVEMBER":  time.November,
		"DECEMBER":  time.December,
	}
)

func (source *pythSource) selectWeightedContracts(at time.Time, contracts []pythContract) []weightedContract {
	active := make([]pythContract, 0, len(contracts))
	for _, current := range contracts {
		if contractActiveOn(current, at) {
			active = append(active, current)
		}
	}
	if len(active) == 0 {
		return nil
	}

	selected := []weightedContract{{contract: active[0], weightBPS: fixedpoint.BasisPointsDenominator}}
	if len(active) == 1 {
		return selected
	}

	secondWeight := int64(0)
	frontWeight := int64(fixedpoint.BasisPointsDenominator)
	localDate := startOfDay(at.In(active[0].schedule.Location()))
	expiryDate := startOfDay(active[0].expiryDate.In(active[0].schedule.Location()))
	rollStart := expiryDate.AddDate(0, 0, -source.config.RollWindowDays)
	switch {
	case localDate.Before(rollStart):
		frontWeight = fixedpoint.BasisPointsDenominator
	case !localDate.Before(expiryDate):
		frontWeight = 0
	default:
		daysRemaining := int64(expiryDate.Sub(localDate) / (24 * time.Hour))
		frontWeight = fixedpoint.MustRoundRatioHalfAway(daysRemaining*fixedpoint.BasisPointsDenominator, int64(source.config.RollWindowDays))
	}
	secondWeight = fixedpoint.BasisPointsDenominator - frontWeight

	selected[0].weightBPS = frontWeight
	selected = append(selected, weightedContract{
		contract:  active[1],
		weightBPS: secondWeight,
	})
	return selected
}

func (source *pythSource) fetchLatestPrices(ctx context.Context, budget FetchBudget, selected []weightedContract) (map[string]pythLatestPrice, time.Time, error) {
	query := url.Values{}
	query.Set("parsed", "true")
	for _, current := range selected {
		if current.weightBPS <= 0 {
			continue
		}
		query.Add("ids[]", current.contract.id)
	}
	if len(query["ids[]"]) == 0 {
		return nil, time.Time{}, invalidQuoteError(source.config.Name, "no weighted pyth contracts to fetch", nil)
	}

	latestURL, err := resolveURL(source.baseURL, "/v2/updates/price/latest", query)
	if err != nil {
		return nil, time.Time{}, invalidQuoteError(source.config.Name, "resolve pyth latest url", err)
	}

	var payload pythLatestResponse
	receivedAt, err := source.fetchJSON(ctx, budget, latestURL, &payload)
	if err != nil {
		return nil, time.Time{}, err
	}

	latestByID := make(map[string]pythLatestPrice, len(payload.Parsed))
	for _, current := range payload.Parsed {
		latestByID[current.ID] = current
	}
	return latestByID, receivedAt, nil
}

func (source *pythSource) liveContracts(now time.Time, selected []weightedContract, latestByID map[string]pythLatestPrice) ([]liveContract, error) {
	live := make([]liveContract, 0, len(selected))
	for _, current := range selected {
		if current.weightBPS <= 0 || !current.contract.schedule.OpenAt(now) {
			continue
		}

		price, ok := latestByID[current.contract.id]
		if !ok {
			continue
		}

		parsedPrice, parsedConf, publishTime, err := parsePythLatest(price.Price)
		if err != nil {
			continue
		}

		age := now.Sub(publishTime)
		if source.stalenessThreshold > 0 && age > source.stalenessThreshold {
			continue
		}
		skew := publishTime.Sub(now)
		if source.maxFutureSkew > 0 && skew > source.maxFutureSkew {
			continue
		}

		live = append(live, liveContract{
			contract:    current.contract,
			weightBPS:   current.weightBPS,
			price:       parsedPrice,
			conf:        parsedConf,
			publishTime: publishTime,
		})
	}

	if len(live) == 0 {
		return nil, invalidQuoteError(source.config.Name, "no live pyth contracts after schedule and freshness checks", nil)
	}
	return live, nil
}

func parsePythExpiry(description string, location *time.Location) (time.Time, error) {
	match := pythExpiryPattern.FindStringSubmatch(strings.ToUpper(strings.TrimSpace(description)))
	if len(match) != 4 {
		return time.Time{}, fmt.Errorf("unsupported description %q", description)
	}

	day, err := strconv.Atoi(match[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("parse expiry day: %w", err)
	}
	month, ok := pythExpiryMonths[match[2]]
	if !ok {
		return time.Time{}, fmt.Errorf("parse expiry month %q", match[2])
	}
	year, err := strconv.Atoi(match[3])
	if err != nil {
		return time.Time{}, fmt.Errorf("parse expiry year: %w", err)
	}
	return time.Date(year, month, day, 0, 0, 0, 0, location), nil
}

func contractActiveOn(contract pythContract, at time.Time) bool {
	localDate := startOfDay(at.In(contract.schedule.Location()))
	expiryDate := startOfDay(contract.expiryDate.In(contract.schedule.Location()))
	return !localDate.After(expiryDate)
}

func startOfDay(at time.Time) time.Time {
	return time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, at.Location())
}

func contractSelectionFields(live []liveContract) []string {
	fields := make([]string, 0, len(live))
	for _, current := range live {
		fields = append(fields, fmt.Sprintf("%s:%d", current.contract.genericSymbol, current.weightBPS))
	}
	return fields
}
