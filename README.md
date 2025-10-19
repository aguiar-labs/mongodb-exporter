# MongoDB Exporter

This project exposes MongoDB operational data for Prometheus. It follows the Go community layout and runs the scraping logic from `cmd/mongodb-exporter`.

## Metrics

- `mongodb_up`: Gauge that flips to `1` when the exporter can connect and ping MongoDB within the configured timeout. A value of `0` signals connectivity or authentication issues.
- `mongodb_slow_queries`: Gauge of slow operations observed since the last polling interval, grouped by database and collection. Spikes point to workloads breaching the configured slow threshold.
- `mongodb_slow_queries_total`: Counter tracking all slow operations seen over the lifetime of the exporter, grouped by database and collection. It helps to confirm if slow queries are a recurring pattern.
- `mongodb_index_access_ops_total`: Counter with cumulative index access operations per database, collection, and index. Large totals indicate frequently accessed indexes or potential hot paths.
- `mongodb_index_access_ops_per_second`: Gauge with the per-interval rate of index access operations. Sudden drops can reveal unused indexes; sharp increases highlight heavy read paths.
- `mongodb_index_size_bytes`: Gauge reporting each index size in bytes. Use it to detect oversized indexes that may affect memory utilization and cache efficiency.
- `mongodb_index_access_since_timestamp`: Gauge storing the Unix timestamp of the first recorded access window for each index. If the value stops updating, the index may be unused.

## Identifying Issues

- Availability problems: `mongodb_up` at `0` means Prometheus scrapes will fail, often due to network, TLS, or authentication errors.
- Slow workloads: Non-zero `mongodb_slow_queries` highlight current pressure; rising `mongodb_slow_queries_total` indicates longer-term performance regressions.
- Index effectiveness: Compare `mongodb_index_access_ops_per_second` with `mongodb_index_size_bytes` to find large indexes that are rarely touched.
- Index churn: Monitor `mongodb_index_access_since_timestamp` together with the rate metric to spot indexes that suddenly lose traffic, which may be candidates for cleanup.

## Running

Configure the exporter through environment variables such as `MONGODB_URI`, `CHECK_INTERVAL`, and `SLOW_MS`. Build and run with:

```bash
go build ./cmd/mongodb-exporter
./mongodb-exporter
```

Scrape metrics from `http://<host>:9216/metrics` (or the address configured via `LISTEN_ADDR`). Integrate the endpoint into Prometheus to persist and alert on the metrics above.

## CI/CD

A GitHub Actions workflow at `.github/workflows/docker-image.yml` builds the container image from `build/Dockerfile` and publishes it to GitHub Container Registry (`ghcr.io/<owner>/<repo>`). Pushes to `main` get a `latest` tag and commits/tags receive SHA- or release-based tags. Configure your Kubernetes deployment to pull one of those published tags, or fork the workflow to target a different registry if required.
