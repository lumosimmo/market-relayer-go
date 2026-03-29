package clock

import "time"

type Clock interface {
	Now() time.Time
}

type Real struct{}

func (Real) Now() time.Time {
	return time.Now().UTC()
}

type Fixed struct {
	Time time.Time
}

func (fixed Fixed) Now() time.Time {
	return fixed.Time
}
