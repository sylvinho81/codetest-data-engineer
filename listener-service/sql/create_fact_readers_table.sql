CREATE TABLE IF NOT EXISTS fact_readers(
  id String,
  saga_id String,
  entity_id String,
  entity_counter Int32,
  readers Int32,
  date_time DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date_time)
ORDER BY (saga_id, entity_id, date_time)