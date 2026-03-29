package markettime

import (
	"testing"
	"time"
)

func TestParseScheduleAndOpenAt(t *testing.T) {
	t.Parallel()

	schedule, err := ParseSchedule("America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0402/0000-1700,0403/C")
	if err != nil {
		t.Fatalf("ParseSchedule() error = %v", err)
	}

	tests := []struct {
		name string
		at   time.Time
		open bool
	}{
		{
			name: "monday maintenance closed",
			at:   time.Date(2026, 3, 23, 21, 30, 0, 0, time.UTC),
			open: false,
		},
		{
			name: "monday after reopen",
			at:   time.Date(2026, 3, 23, 22, 15, 0, 0, time.UTC),
			open: true,
		},
		{
			name: "friday before close",
			at:   time.Date(2026, 3, 27, 20, 30, 0, 0, time.UTC),
			open: true,
		},
		{
			name: "friday after close",
			at:   time.Date(2026, 3, 27, 21, 30, 0, 0, time.UTC),
			open: false,
		},
		{
			name: "sunday before open",
			at:   time.Date(2026, 3, 22, 21, 30, 0, 0, time.UTC),
			open: false,
		},
		{
			name: "sunday after open",
			at:   time.Date(2026, 3, 22, 22, 30, 0, 0, time.UTC),
			open: true,
		},
		{
			name: "good friday closed override",
			at:   time.Date(2026, 4, 3, 15, 0, 0, 0, time.UTC),
			open: false,
		},
		{
			name: "pre-good-friday early close still open",
			at:   time.Date(2026, 4, 2, 19, 0, 0, 0, time.UTC),
			open: true,
		},
		{
			name: "pre-good-friday after override close",
			at:   time.Date(2026, 4, 2, 22, 0, 0, 0, time.UTC),
			open: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := schedule.OpenAt(tt.at); got != tt.open {
				t.Fatalf("schedule.OpenAt(%s) = %t, want %t", tt.at.UTC().Format(time.RFC3339), got, tt.open)
			}
		})
	}
}

func TestParseScheduleRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		"America/New_York",
		"America/New_York;0000-1700",
		"America/New_York;0000-2500,0000-1700,0000-1700,0000-1700,0000-1700,C,1800-2400",
		"America/New_York;0000-1700,0000-1700,0000-1700,0000-1700,0000-1700,C,1800-2400;0403",
		"Missing/Zone;0000-1700,0000-1700,0000-1700,0000-1700,0000-1700,C,1800-2400",
	}

	for _, input := range tests {
		input := input
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			if _, err := ParseSchedule(input); err == nil {
				t.Fatalf("ParseSchedule(%q) error = nil, want error", input)
			}
		})
	}
}

func TestParseMinute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		end   bool
		want  int
		ok    bool
	}{
		{name: "midnight", input: "0000", want: 0, ok: true},
		{name: "afternoon", input: "1345", want: 13*60 + 45, ok: true},
		{name: "end of day", input: "2400", end: true, want: 24 * 60, ok: true},
		{name: "invalid start 2400", input: "2400", ok: false},
		{name: "invalid format", input: "9:30", ok: false},
		{name: "invalid digits", input: "12x0", ok: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseMinute(tt.input, tt.end)
			if tt.ok {
				if err != nil {
					t.Fatalf("parseMinute(%q, %t) error = %v", tt.input, tt.end, err)
				}
				if got != tt.want {
					t.Fatalf("parseMinute(%q, %t) = %d, want %d", tt.input, tt.end, got, tt.want)
				}
				return
			}

			if err == nil {
				t.Fatalf("parseMinute(%q, %t) error = nil, want error", tt.input, tt.end)
			}
		})
	}
}

func TestWeekdayIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		weekday time.Weekday
		want    int
	}{
		{weekday: time.Monday, want: 0},
		{weekday: time.Tuesday, want: 1},
		{weekday: time.Wednesday, want: 2},
		{weekday: time.Thursday, want: 3},
		{weekday: time.Friday, want: 4},
		{weekday: time.Saturday, want: 5},
		{weekday: time.Sunday, want: 6},
	}

	for _, tt := range tests {
		if got := weekdayIndex(tt.weekday); got != tt.want {
			t.Fatalf("weekdayIndex(%s) = %d, want %d", tt.weekday, got, tt.want)
		}
	}
}
