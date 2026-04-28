-- Schema for run metadata and ETL results (PostgreSQL)
CREATE TABLE IF NOT EXISTS run_metadata (
  id SERIAL PRIMARY KEY,
  run_id TEXT UNIQUE NOT NULL,
  pipeline_name TEXT NOT NULL,
  batch_size INTEGER NOT NULL,
  avg_batch_size DOUBLE PRECISION,
  total_records BIGINT,
  malformed_records BIGINT,
  total_batches INTEGER,
  runtime_ms BIGINT,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS etl_results (
  id SERIAL PRIMARY KEY,
  run_id TEXT NOT NULL,
  pipeline_name TEXT NOT NULL,
  batch_id INTEGER,
  query_name TEXT NOT NULL,
  k1 TEXT,
  k2 TEXT,
  m1 DOUBLE PRECISION,
  m2 DOUBLE PRECISION,
  m3 DOUBLE PRECISION,
  m4 DOUBLE PRECISION,
  created_at TIMESTAMP DEFAULT now()
);
