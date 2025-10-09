-- TimescaleDB schema for data-db per-cycle writes (auto-applied on first start)
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS bibimbap (
  time   TIMESTAMPTZ NOT NULL,
  device TEXT        NOT NULL,
  data   JSONB       NOT NULL
);

SELECT public.create_hypertable('bibimbap', 'time',
  partitioning_column => 'device',
  number_partitions => 4,
  if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS bibimbap_time_idx ON bibimbap (time DESC);
CREATE INDEX IF NOT EXISTS bibimbap_device_idx ON bibimbap (device);
