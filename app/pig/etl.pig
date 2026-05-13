%default INPUT ''
%default OUTPUT ''
%default BATCH_SIZE_DAYS 1

raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

parsed = FOREACH raw GENERATE
  REGEX_EXTRACT(
    line,
    '^(\\\\S+) \\\\S+ \\\\S+ \\\\[(\\\\S+\\\\s-\\\\d{4})\\\\] "(\\\\S+) (\\\\S+) (\\\\S+)" (\\\\d{3}) (\\\\S+)',
    1
  ) AS host,

  REGEX_EXTRACT(
    line,
    '^(\\\\S+) \\\\S+ \\\\S+ \\\\[(\\\\S+\\\\s-\\\\d{4})\\\\] "(\\\\S+) (\\\\S+) (\\\\S+)" (\\\\d{3}) (\\\\S+)',
    2
  ) AS ts,

  REGEX_EXTRACT(
    line,
    '^(\\\\S+) \\\\S+ \\\\S+ \\\\[(\\\\S+\\\\s-\\\\d{4})\\\\] "(\\\\S+) (\\\\S+) (\\\\S+)" (\\\\d{3}) (\\\\S+)',
    4
  ) AS resource_path,

  (int)(
    REGEX_EXTRACT(
      line,
      '^(\\\\S+) \\\\S+ \\\\S+ \\\\[(\\\\S+\\\\s-\\\\d{4})\\\\] "(\\\\S+) (\\\\S+) (\\\\S+)" (\\\\d{3}) (\\\\S+)',
      6
    )
  ) AS status_code,

  REPLACE(
    REGEX_EXTRACT(
      line,
      '^(\\\\S+) \\\\S+ \\\\S+ \\\\[(\\\\S+\\\\s-\\\\d{4})\\\\] "(\\\\S+) (\\\\S+) (\\\\S+)" (\\\\d{3}) (\\\\S+)',
      7
    ),
    '-',
    '0'
  ) AS bytes_str;

good = FILTER parsed BY
  host IS NOT NULL AND
  ts IS NOT NULL AND
  status_code IS NOT NULL;

bad = FILTER parsed BY
  host IS NULL OR
  ts IS NULL OR
  status_code IS NULL;

with_parts = FOREACH good GENERATE
  host,
  resource_path,
  status_code,
  (long)(bytes_str) AS bytes_val,

  REGEX_EXTRACT(
    ts,
    '^(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2})',
    1
  ) AS day_s,

  REGEX_EXTRACT(
    ts,
    '^(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2})',
    2
  ) AS mon_s,

  REGEX_EXTRACT(
    ts,
    '^(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2})',
    3
  ) AS year_s,

  (int)(
    REGEX_EXTRACT(
      ts,
      '^(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2})',
      4
    )
  ) AS log_hour;

with_mon = FOREACH with_parts GENERATE
  host,
  resource_path,
  status_code,
  bytes_val,
  log_hour,
  year_s,
  day_s,

  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    mon_s,
    'Jan', '01'),
    'Feb', '02'),
    'Mar', '03'),
    'Apr', '04'),
    'May', '05'),
    'Jun', '06'),
    'Jul', '07'),
    'Aug', '08'),
    'Sep', '09'),
    'Oct', '10'),
    'Nov', '11'),
    'Dec', '12') AS mon_num;

enriched = FOREACH with_mon GENERATE
  host,
  resource_path,
  status_code,
  bytes_val,
  log_hour,

  CONCAT(
    year_s,
    CONCAT(
      '-',
      CONCAT(
        mon_num,
        CONCAT('-', day_s)
      )
    )
  ) AS log_date,

  CONCAT(
    year_s,
    CONCAT('-', mon_num)
  ) AS log_month;

-- Q1
q1_grp = GROUP enriched BY (log_date, status_code);

q1 = FOREACH q1_grp GENERATE
  FLATTEN(group) AS (log_date, status_code),
  COUNT(enriched) AS request_count,
  SUM(enriched.bytes_val) AS total_bytes;

q1_sorted = ORDER q1 BY log_date ASC, status_code ASC;

STORE q1_sorted
INTO '$OUTPUT/q1'
USING PigStorage('\t');

-- Q2
q2_grp = GROUP enriched BY (log_month, resource_path);

q2 = FOREACH q2_grp {
  hosts = DISTINCT enriched.host;

  GENERATE
    FLATTEN(group) AS (log_month, resource_path),
    COUNT(enriched) AS request_count,
    SUM(enriched.bytes_val) AS total_bytes,
    COUNT(hosts) AS distinct_host_count;
};

q2_sorted = ORDER q2 BY log_month ASC, request_count DESC;

STORE q2_sorted
INTO '$OUTPUT/q2'
USING PigStorage('\t');

-- Q3
q3_grp = GROUP enriched BY (log_date, log_hour);

q3 = FOREACH q3_grp {
  errors = FILTER enriched BY status_code >= 400;
  err_hosts = DISTINCT errors.host;

  GENERATE
    FLATTEN(group) AS (log_date, log_hour),
    COUNT(errors) AS error_request_count,
    COUNT(enriched) AS total_request_count,
    ((double)COUNT(errors)) / ((double)COUNT(enriched)) AS error_rate,
    COUNT(err_hosts) AS distinct_error_hosts;
};

q3_sorted = ORDER q3 BY log_date ASC, log_hour ASC;

STORE q3_sorted
INTO '$OUTPUT/q3'
USING PigStorage('\t');

-- Malformed summary
all_grp = GROUP parsed ALL;
bad_grp = GROUP bad ALL;

all_cnt = FOREACH all_grp GENERATE
  COUNT(parsed) AS total;

bad_cnt = FOREACH bad_grp GENERATE
  COUNT(bad) AS malformed;

mal_cross = CROSS all_cnt, bad_cnt;

malformed_summary = FOREACH mal_cross GENERATE
  all_cnt::total AS total,
  bad_cnt::malformed AS malformed;

STORE malformed_summary
INTO '$OUTPUT/malformed_summary'
USING PigStorage('\t');