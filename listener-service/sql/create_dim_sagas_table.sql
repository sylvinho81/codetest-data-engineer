CREATE TABLE IF NOT EXISTS dim_saga(
  saga_id String,
  name String
)
ENGINE = Join(ALL, INNER, saga_id);


