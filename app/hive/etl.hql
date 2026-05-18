-- Hive ETL script for NASA HTTP access logs.
--
-- Required hiveconf variables:
--   RUN_ID      Unique ETL run id
--   INPUT       Local path or directory containing NASA log files, including .gz files
--   OUTPUT      Local output directory for tab-delimited results
--   BATCH_MODE  days or records
--   BATCH_SIZE  Number of days or records per batch

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.vectorized.execution.enabled=false;

CREATE DATABASE IF NOT EXISTS multietl;
USE multietl;

DROP TABLE IF EXISTS raw_logs_stage;
DROP TABLE IF EXISTS parsed_logs_stage;
DROP TABLE IF EXISTS date_batches_stage;
DROP TABLE IF EXISTS batched_logs_stage;

CREATE TABLE raw_logs_stage (
  line STRING
)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:INPUT}' OVERWRITE INTO TABLE raw_logs_stage;

CREATE TABLE parsed_logs_stage
STORED AS ORC
AS
SELECT
  '${hiveconf:RUN_ID}' AS run_id,
  record_seq,
  host,
  CASE
    WHEN valid = 1 AND ts_epoch IS NOT NULL AND bytes_ok <> '' THEN CONCAT(
      SUBSTR(ts, 8, 4), '-',
      CASE LOWER(SUBSTR(ts, 4, 3))
        WHEN 'jan' THEN '01'
        WHEN 'feb' THEN '02'
        WHEN 'mar' THEN '03'
        WHEN 'apr' THEN '04'
        WHEN 'may' THEN '05'
        WHEN 'jun' THEN '06'
        WHEN 'jul' THEN '07'
        WHEN 'aug' THEN '08'
        WHEN 'sep' THEN '09'
        WHEN 'oct' THEN '10'
        WHEN 'nov' THEN '11'
        WHEN 'dec' THEN '12'
      END,
      '-', SUBSTR(ts, 1, 2)
    )
    ELSE NULL
  END AS log_date,
  CASE
    WHEN valid = 1 AND ts_epoch IS NOT NULL AND bytes_ok <> '' THEN CONCAT(
      SUBSTR(ts, 8, 4), '-',
      CASE LOWER(SUBSTR(ts, 4, 3))
        WHEN 'jan' THEN '01'
        WHEN 'feb' THEN '02'
        WHEN 'mar' THEN '03'
        WHEN 'apr' THEN '04'
        WHEN 'may' THEN '05'
        WHEN 'jun' THEN '06'
        WHEN 'jul' THEN '07'
        WHEN 'aug' THEN '08'
        WHEN 'sep' THEN '09'
        WHEN 'oct' THEN '10'
        WHEN 'nov' THEN '11'
        WHEN 'dec' THEN '12'
      END
    )
    ELSE NULL
  END AS log_month,
  CASE WHEN valid = 1 AND ts_epoch IS NOT NULL AND bytes_ok <> '' THEN CAST(SUBSTR(ts, 13, 2) AS INT) ELSE NULL END AS log_hour,
  method,
  resource_path,
  protocol,
  CASE WHEN valid = 1 AND ts_epoch IS NOT NULL AND bytes_ok <> '' THEN CAST(status_str AS INT) ELSE NULL END AS status_code,
  CASE
    WHEN valid = 1 AND ts_epoch IS NOT NULL AND bytes_ok = '-' THEN CAST(0 AS BIGINT)
    WHEN valid = 1 AND ts_epoch IS NOT NULL AND bytes_ok <> '' THEN CAST(bytes_ok AS BIGINT)
    ELSE CAST(0 AS BIGINT)
  END AS bytes_val,
  CASE WHEN valid = 1 AND ts_epoch IS NOT NULL AND bytes_ok <> '' THEN CAST(0 AS INT) ELSE CAST(1 AS INT) END AS malformed,
  line AS raw_line
FROM (
  SELECT
    ROW_NUMBER() OVER (ORDER BY INPUT__FILE__NAME, BLOCK__OFFSET__INSIDE__FILE) AS record_seq,
    line,
    CASE WHEN line RLIKE '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\S+\\s-\\d{4})\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?.*"\\s+(\\d{3})\\s+(\\S+)' THEN 1 ELSE 0 END AS valid,
    REGEXP_EXTRACT(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\S+\\s-\\d{4})\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?.*"\\s+(\\d{3})\\s+(\\S+)', 1) AS host,
    REGEXP_EXTRACT(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\S+\\s-\\d{4})\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?.*"\\s+(\\d{3})\\s+(\\S+)', 2) AS ts,
    REGEXP_EXTRACT(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\S+\\s-\\d{4})\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?.*"\\s+(\\d{3})\\s+(\\S+)', 3) AS method,
    REGEXP_EXTRACT(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\S+\\s-\\d{4})\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?.*"\\s+(\\d{3})\\s+(\\S+)', 4) AS resource_path,
    REGEXP_EXTRACT(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\S+\\s-\\d{4})\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?.*"\\s+(\\d{3})\\s+(\\S+)', 5) AS protocol,
    REGEXP_EXTRACT(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\S+\\s-\\d{4})\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?.*"\\s+(\\d{3})\\s+(\\S+)', 6) AS status_str,
    REGEXP_EXTRACT(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\S+\\s-\\d{4})\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?.*"\\s+(\\d{3})\\s+(\\S+)', 7) AS bytes_str,
    REGEXP_EXTRACT(REGEXP_EXTRACT(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\S+\\s-\\d{4})\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?.*"\\s+(\\d{3})\\s+(\\S+)', 7), '^(\\d+|-)$', 1) AS bytes_ok,
    UNIX_TIMESTAMP(REGEXP_EXTRACT(line, '^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\S+\\s-\\d{4})\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?.*"\\s+(\\d{3})\\s+(\\S+)', 2), 'dd/MMM/yyyy:HH:mm:ss Z') AS ts_epoch
  FROM raw_logs_stage
) parsed;

CREATE TABLE date_batches_stage
STORED AS ORC
AS
SELECT
  log_date,
  CAST(FLOOR((date_seq - 1) / CAST('${hiveconf:BATCH_SIZE}' AS INT)) + 1 AS INT) AS day_batch_id
FROM (
  SELECT
    log_date,
    ROW_NUMBER() OVER (ORDER BY log_date) AS date_seq
  FROM (
    SELECT DISTINCT log_date
    FROM parsed_logs_stage
    WHERE log_date IS NOT NULL
  ) dates
) ranked_dates;

CREATE TABLE batched_logs_stage
STORED AS ORC
AS
SELECT
  b.run_id,
  CASE
    WHEN '${hiveconf:BATCH_MODE}' = 'records'
      THEN CAST(FLOOR((b.record_seq - 1) / CAST('${hiveconf:BATCH_SIZE}' AS INT)) + 1 AS INT)
    ELSE COALESCE(b.day_batch_id, b.previous_day_batch_id, 1)
  END AS batch_id,
  b.record_seq,
  b.host,
  b.log_date,
  b.log_month,
  b.log_hour,
  b.method,
  b.resource_path,
  b.protocol,
  b.status_code,
  b.bytes_val,
  b.malformed,
  b.raw_line
FROM (
  SELECT
    p.*,
    d.day_batch_id,
    MAX(d.day_batch_id) OVER (
      ORDER BY p.record_seq
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS previous_day_batch_id
  FROM parsed_logs_stage p
  LEFT JOIN date_batches_stage d
    ON p.log_date = d.log_date
) b;

INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/malformed_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
  COUNT(*) AS total_records,
  SUM(malformed) AS malformed_records
FROM batched_logs_stage;

INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/batch_counts'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
  batch_id,
  COUNT(*) AS total_records,
  SUM(malformed) AS malformed_records
FROM batched_logs_stage
WHERE batch_id > 0
GROUP BY batch_id;

INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/batch_date_counts'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
  batch_id,
  log_date,
  COUNT(*) AS total_records,
  SUM(malformed) AS malformed_records
FROM batched_logs_stage
WHERE batch_id > 0 AND log_date IS NOT NULL
GROUP BY batch_id, log_date;

INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
  batch_id,
  log_date,
  status_code,
  COUNT(*) AS request_count,
  SUM(bytes_val) AS total_bytes
FROM batched_logs_stage
WHERE batch_id > 0 AND malformed = 0
GROUP BY batch_id, log_date, status_code;

INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
  batch_id,
  log_month,
  resource_path,
  request_count,
  total_bytes,
  distinct_host_count
FROM (
  SELECT
    ranked.*,
    ROW_NUMBER() OVER (PARTITION BY batch_id, log_month ORDER BY request_count DESC) AS resource_rank
  FROM (
    SELECT
      batch_id,
      log_month,
      resource_path,
      COUNT(*) AS request_count,
      SUM(bytes_val) AS total_bytes,
      COUNT(DISTINCT host) AS distinct_host_count
    FROM batched_logs_stage
    WHERE batch_id > 0 AND malformed = 0
    GROUP BY batch_id, log_month, resource_path
  ) ranked
) top_resources
WHERE resource_rank <= 20;

INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
  batch_id,
  log_date,
  log_hour,
  SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,
  COUNT(*) AS total_request_count,
  CASE
    WHEN COUNT(*) = 0 THEN 0.0
    ELSE CAST(SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)
  END AS error_rate,
  COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host ELSE NULL END) AS distinct_error_hosts
FROM batched_logs_stage
WHERE batch_id > 0 AND malformed = 0
GROUP BY batch_id, log_date, log_hour;
