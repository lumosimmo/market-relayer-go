package markettime

import (
	"fmt"
	"strings"
	"time"
)

const weeklyDays = 7

type TimeRange struct {
	StartMinute int
	EndMinute   int
}

type Schedule struct {
	location  *time.Location
	weekly    [weeklyDays][]TimeRange
	overrides map[string][]TimeRange
}

func ParseSchedule(input string) (Schedule, error) {
	parts := strings.Split(strings.TrimSpace(input), ";")
	if len(parts) < 2 || len(parts) > 3 {
		return Schedule{}, fmt.Errorf("markettime: invalid schedule %q", input)
	}

	location, err := time.LoadLocation(strings.TrimSpace(parts[0]))
	if err != nil {
		return Schedule{}, fmt.Errorf("markettime: load location: %w", err)
	}

	weeklyParts := strings.Split(strings.TrimSpace(parts[1]), ",")
	if len(weeklyParts) != weeklyDays {
		return Schedule{}, fmt.Errorf("markettime: weekly schedule requires %d day entries", weeklyDays)
	}

	var weekly [weeklyDays][]TimeRange
	for index, rawDay := range weeklyParts {
		ranges, err := parseDayRanges(rawDay)
		if err != nil {
			return Schedule{}, fmt.Errorf("markettime: weekly day %d: %w", index, err)
		}
		weekly[index] = ranges
	}

	overrides := make(map[string][]TimeRange)
	if len(parts) == 3 && strings.TrimSpace(parts[2]) != "" {
		for _, rawOverride := range strings.Split(strings.TrimSpace(parts[2]), ",") {
			rawOverride = strings.TrimSpace(rawOverride)
			if rawOverride == "" {
				continue
			}

			overrideParts := strings.SplitN(rawOverride, "/", 2)
			if len(overrideParts) != 2 {
				return Schedule{}, fmt.Errorf("markettime: invalid override %q", rawOverride)
			}
			key := strings.TrimSpace(overrideParts[0])
			if len(key) != 4 {
				return Schedule{}, fmt.Errorf("markettime: invalid override date %q", key)
			}
			ranges, err := parseDayRanges(overrideParts[1])
			if err != nil {
				return Schedule{}, fmt.Errorf("markettime: override %q: %w", key, err)
			}
			overrides[key] = ranges
		}
	}

	return Schedule{
		location:  location,
		weekly:    weekly,
		overrides: overrides,
	}, nil
}

func (schedule Schedule) Location() *time.Location {
	return schedule.location
}

func (schedule Schedule) OpenAt(at time.Time) bool {
	if schedule.location == nil {
		return false
	}

	local := at.In(schedule.location)
	ranges, ok := schedule.overrides[local.Format("0102")]
	if !ok {
		ranges = schedule.weekly[weekdayIndex(local.Weekday())]
	}

	minute := local.Hour()*60 + local.Minute()
	for _, current := range ranges {
		if minute >= current.StartMinute && minute < current.EndMinute {
			return true
		}
	}
	return false
}

func parseDayRanges(input string) ([]TimeRange, error) {
	spec := strings.TrimSpace(input)
	switch spec {
	case "", "C":
		return nil, nil
	}

	segments := strings.Split(spec, "&")
	ranges := make([]TimeRange, 0, len(segments))
	for _, segment := range segments {
		bounds := strings.SplitN(strings.TrimSpace(segment), "-", 2)
		if len(bounds) != 2 {
			return nil, fmt.Errorf("invalid time range %q", segment)
		}
		start, err := parseMinute(bounds[0], false)
		if err != nil {
			return nil, err
		}
		end, err := parseMinute(bounds[1], true)
		if err != nil {
			return nil, err
		}
		if start >= end {
			return nil, fmt.Errorf("invalid time range %q", segment)
		}
		ranges = append(ranges, TimeRange{
			StartMinute: start,
			EndMinute:   end,
		})
	}

	return ranges, nil
}

func parseMinute(input string, end bool) (int, error) {
	value := strings.TrimSpace(input)
	if len(value) != 4 {
		return 0, fmt.Errorf("invalid time value %q", input)
	}

	if value == "2400" {
		if !end {
			return 0, fmt.Errorf("invalid start time %q", input)
		}
		return 24 * 60, nil
	}

	parsed, err := time.Parse("1504", value)
	if err != nil {
		return 0, fmt.Errorf("invalid time value %q", input)
	}
	return parsed.Hour()*60 + parsed.Minute(), nil
}

func weekdayIndex(weekday time.Weekday) int {
	return (int(weekday) + weeklyDays - 1) % weeklyDays
}
