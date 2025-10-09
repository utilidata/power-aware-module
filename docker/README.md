**`docker/README.md`**


# Docker images

- `Dockerfile.data-exporter`  — builds and runs the data-exporter Prometheus exporter.
- `Dockerfile.data-db`   — builds and runs data-db (writes to TimescaleDB).
- `Dockerfile.data-replay` — tiny Python ZeroMQ publisher used for demos.

Each uses a small Debian runtime; data-db installs `libssl3` for Postgres TLS.
