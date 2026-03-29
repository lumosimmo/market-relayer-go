GO ?= go
CONFIG ?= configs/markets.example.yaml
CGO_ENABLED ?= 0
GOTOOLCHAIN ?= local

.PHONY: test test-local run lint-lite validate-config migrate-store store-version smoke-fixtures bench test-compose smoke-compose integration-compose

test:
	$(MAKE) test-compose

test-local:
	MARKET_RELAYER_SKIP_POSTGRES_TESTS=1 CGO_ENABLED=$(CGO_ENABLED) GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) test ./...

run:
	CGO_ENABLED=$(CGO_ENABLED) GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) run ./cmd/market-relayer -config $(CONFIG)

migrate-store:
	CGO_ENABLED=$(CGO_ENABLED) GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) run ./cmd/market-relayer -migrate-store -config $(CONFIG)

store-version:
	CGO_ENABLED=$(CGO_ENABLED) GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) run ./cmd/market-relayer -store-version -config $(CONFIG)

lint-lite:
	@UNFORMATTED="$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*' -not -path './tmp/*'))"; \
	if [ -n "$$UNFORMATTED" ]; then \
		echo "$$UNFORMATTED"; \
		exit 1; \
	fi
	CGO_ENABLED=$(CGO_ENABLED) GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) vet ./...

validate-config:
	CGO_ENABLED=$(CGO_ENABLED) GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) run ./cmd/market-relayer -validate-config -config $(CONFIG)

smoke-fixtures:
	CGO_ENABLED=$(CGO_ENABLED) GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) test ./internal/testutil -run TestFixtureSmokeCoversRecoveryAndCrashBoundaries

bench:
	CGO_ENABLED=$(CGO_ENABLED) GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) test ./internal/testutil -run TestFixtureCycleBudgetsStayWithinTargets -bench BenchmarkFixtureCycleBudgets -benchmem

test-compose:
	@set -e; \
	trap 'docker compose -f docker-compose.test.yml down -v' EXIT; \
	docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from test-runner test-runner

smoke-compose:
	@set -e; \
	docker compose -f docker-compose.prod-template.yml config >/dev/null; \
	trap 'docker compose -f docker-compose.test.yml down -v' EXIT; \
	docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from smoke-runner smoke-runner

integration-compose:
	$(MAKE) test-compose
	$(MAKE) smoke-compose
