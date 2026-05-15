set hive.execution.engine=mr;
set mapreduce.framework.name=local;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

DROP TABLE IF EXISTS parsed_logs;
CREATE EXTERNAL TABLE parsed_logs (
    host STRING,
    ts STRING,
    request STRING,
    status_code STRING,
    bytes_str STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "^(\\S+) \\S+ \\S+ \\[(\\S+\\s-\\d{4})\\] \"([^\"]*)\" (\\d{3}) (\\S+)"
)
STORED AS TEXTFILE
LOCATION '${INPUT}';

DROP VIEW IF EXISTS enriched;
CREATE VIEW enriched AS
SELECT 
    host,
    split(request, ' ')[1] AS resource_path,
    CAST(status_code AS INT) AS status_code,
    CAST(regexp_replace(bytes_str, '-', '0') AS BIGINT) AS bytes_val,
    CAST(regexp_extract(ts, '^(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 4) AS INT) AS log_hour,
    concat(
        regexp_extract(ts, '^(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 3),
        '-',
        CASE regexp_extract(ts, '^(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 2)
            WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03' WHEN 'Apr' THEN '04'
            WHEN 'May' THEN '05' WHEN 'Jun' THEN '06' WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08'
            WHEN 'Sep' THEN '09' WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
        END,
        '-',
        regexp_extract(ts, '^(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 1)
    ) AS log_date,
    concat(
        regexp_extract(ts, '^(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 3),
        '-',
        CASE regexp_extract(ts, '^(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 2)
            WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03' WHEN 'Apr' THEN '04'
            WHEN 'May' THEN '05' WHEN 'Jun' THEN '06' WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08'
            WHEN 'Sep' THEN '09' WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
        END
    ) AS log_month
FROM parsed_logs
WHERE host IS NOT NULL AND status_code IS NOT NULL;

INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT}/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT log_date, status_code, COUNT(*), SUM(bytes_val)
FROM enriched
GROUP BY log_date, status_code
ORDER BY log_date ASC, status_code ASC;

INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT}/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT log_month, resource_path, COUNT(*) as request_count, SUM(bytes_val), COUNT(DISTINCT host)
FROM enriched
GROUP BY log_month, resource_path
ORDER BY log_month ASC, request_count DESC;

INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT}/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    log_date, 
    log_hour, 
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END), 
    COUNT(*), 
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) / COUNT(*),
    COUNT(DISTINCT CASE WHEN status_code >= 400 THEN host ELSE NULL END)
FROM enriched
GROUP BY log_date, log_hour
ORDER BY log_date ASC, log_hour ASC;

INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT}/malformed_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT a.tot, b.mal
FROM 
  (SELECT COUNT(*) as tot FROM parsed_logs) a
CROSS JOIN
  (SELECT COUNT(*) as mal FROM parsed_logs WHERE host IS NULL) b;
