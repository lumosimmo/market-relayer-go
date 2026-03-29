package store

import "time"

type Lease struct {
	Market       string    `json:"market"`
	Owner        string    `json:"owner"`
	FencingToken int64     `json:"fencing_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}
