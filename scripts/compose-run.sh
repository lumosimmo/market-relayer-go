#!/bin/sh
set -eu

case "${1:-}" in
  test)
    go test ./... -count=1
    go test -covermode=atomic -coverprofile=/tmp/store.cover ./internal/store -count=1
    go tool cover -func=/tmp/store.cover
    ;;
  smoke)
    go test ./internal/app -run TestPostgresHAUsesSingleOwnerPerMarket -count=1
    ;;
  *)
    echo "usage: $0 <test|smoke>" >&2
    exit 2
    ;;
esac
