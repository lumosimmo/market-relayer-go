# Deploy Assets

This directory contains the local observability assets for the relayer:

- [prometheus.yml](./prometheus.yml) for a single local relayer on `127.0.0.1:8080`
- [prometheus.compose.yml](./prometheus.compose.yml) for the multi-service Compose template
- [grafana-dashboard.json](./grafana-dashboard.json) for the shipped metric families

## Local Usage

Start the relayer first. The sample config expects Postgres plus an HTTP sink on `127.0.0.1:18080`, unless you switch to the file sink.

```bash
make run
```

Start Prometheus:

```bash
prometheus --config.file=deploy/prometheus.yml --web.listen-address=127.0.0.1:9090
```

Validate the Prometheus configs with `promtool` if needed:

```bash
promtool check config deploy/prometheus.yml
promtool check config deploy/prometheus.compose.yml
```

Import [grafana-dashboard.json](./grafana-dashboard.json) into Grafana and point it at Prometheus. The dashboard covers scrape health, source freshness, phase latency, publishability, rejection and cooldown events, and recovery activity.

`/statusz` remains the source of truth for per-market state and availability reasons.

For a production-like local stack, the observability profile in [`docker-compose.prod-template.yml`](../docker-compose.prod-template.yml) starts Prometheus and Grafana around the multi-replica template:

```bash
docker compose -f docker-compose.prod-template.yml --profile observability up
```
