package app

import (
	"context"
	"time"
)

const defaultCleanupTimeout = 5 * time.Second

func cleanupContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = defaultCleanupTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}
