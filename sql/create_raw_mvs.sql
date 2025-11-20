-- Materialized Views для копирования данных из Kafka в MergeTree таблицы

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_browser_events_kafka
TO raw.browser_events
AS SELECT * FROM raw.browser_events_kafka;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_location_events_kafka
TO raw.location_events
AS SELECT * FROM raw.location_events_kafka;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_device_events_kafka
TO raw.device_events
AS SELECT * FROM raw.device_events_kafka;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_geo_events_kafka
TO raw.geo_events
AS SELECT * FROM raw.geo_events_kafka;