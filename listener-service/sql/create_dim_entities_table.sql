CREATE TABLE IF NOT EXISTS dim_entity(
  entity_id String,
  name String
)
ENGINE = Join(ALL, INNER, entity_id);