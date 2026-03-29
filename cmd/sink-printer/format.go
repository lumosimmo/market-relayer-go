package main

import (
	"strconv"
	"strings"

	"github.com/lumosimmo/market-relayer-go/internal/relayer"
)

func formatSelectedSources(sources []string) string {
	if len(sources) == 0 {
		return "[]"
	}
	return "[" + strings.Join(sources, ",") + "]"
}

func formatPricePayload(price relayer.PricePayload) string {
	if price.Value == nil {
		reason := string(price.Reason)
		if reason == "" {
			reason = "none"
		}
		return string(price.Status) + "(" + reason + ")"
	}
	return formatScaledValue(*price.Value, price.Scale)
}

func formatScaledValue(value int64, scale int32) string {
	if scale <= 0 {
		return strconv.FormatInt(value, 10)
	}

	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}

	digits := strconv.FormatInt(value, 10)
	scaleInt := int(scale)
	if len(digits) <= scaleInt {
		digits = strings.Repeat("0", scaleInt-len(digits)+1) + digits
	}
	split := len(digits) - scaleInt
	return sign + digits[:split] + "." + digits[split:]
}
