%default INPUT ''
%default OUTPUT ''
%default BATCH_SIZE_DAYS 1

raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

pattern = '^(\\S+) \\S+ \\S+ \\[(\\S+\\s-\\d{4})\\] "(\\S+) (\\S+) (\\S+)" (\\d{3}) (\\S+)';

parsed = FOREACH raw {
  valid = line MATCHES pattern;
  host = (valid ? REGEX_EXTRACT(line, pattern, 1) : null);
  ts = (valid ? REGEX_EXTRACT(line, pattern, 2) : null);
  method = (valid ? REGEX_EXTRACT(line, pattern, 3) : null);
  resource_path = (valid ? REGEX_EXTRACT(line, pattern, 4) : null);
  protocol = (valid ? REGEX_EXTRACT(line, pattern, 5) : null);
  status_str = (valid ? REGEX_EXTRACT(line, pattern, 6) : null);
  bytes_str = (valid ? REGEX_EXTRACT(line, pattern, 7) : null);
  ts_dt = (valid ? ToDate(ts, 'dd/MMM/yyyy:HH:mm:ss Z') : null);
  log_date = (ts_dt is null ? null : ToString(ts_dt, 'yyyy-MM-dd'));
  log_month = (log_date is null ? null : SUBSTRING(log_date, 0, 7));
  log_hour = (valid ? (int)ToInt(SUBSTRING(ts, 12, 14)) : null);
  status_code = (valid ? (int)ToInt(status_str) : null);
  bytes_val = (valid ? (bytes_str == '-' ? 0L : (long)ToLong(bytes_str)) : null);
  malformed = ((valid AND ts_dt is not null) ? 0 : 1);
  GENERATE host, log_date, log_month, log_hour, method, resource_path, protocol, status_code, bytes_val, malformed;
};

parsed_with_date = FILTER parsed BY log_date is not null;
parsed_good = FILTER parsed_with_date BY malformed == 0;

summary_group = GROUP parsed ALL;
summary = FOREACH summary_group GENERATE COUNT(parsed) AS total_records, SUM(parsed.malformed) AS malformed_records;
STORE summary INTO '$OUTPUT/malformed_summary' USING PigStorage('\t');

date_group = GROUP parsed_with_date BY log_date;
date_counts = FOREACH date_group GENERATE group AS log_date, COUNT(parsed_with_date) AS total_records, SUM(parsed_with_date.malformed) AS malformed_records;
STORE date_counts INTO '$OUTPUT/date_counts' USING PigStorage('\t');

q1_group = GROUP parsed_good BY (log_date, status_code);
q1 = FOREACH q1_group GENERATE group.log_date AS log_date, group.status_code AS status_code, COUNT(parsed_good) AS request_count, SUM(parsed_good.bytes_val) AS total_bytes;
STORE q1 INTO '$OUTPUT/q1' USING PigStorage('\t');

q2_group = GROUP parsed_good BY resource_path;
q2_counts = FOREACH q2_group GENERATE group AS resource_path, COUNT(parsed_good) AS request_count, SUM(parsed_good.bytes_val) AS total_bytes, COUNT(DISTINCT parsed_good.host) AS distinct_host_count;
q2_ordered = ORDER q2_counts BY request_count DESC;
q2_top = LIMIT q2_ordered 20;
STORE q2_top INTO '$OUTPUT/q2' USING PigStorage('\t');

errors = FOREACH parsed_good GENERATE log_date, log_hour, host, (status_code >= 400 AND status_code <= 599 ? 1 : 0) AS is_error;
q3_group = GROUP errors BY (log_date, log_hour);
q3 = FOREACH q3_group {
  total_requests = COUNT(errors);
  error_bag = FILTER errors BY is_error == 1;
  error_count = COUNT(error_bag);
  distinct_err_hosts = COUNT(DISTINCT error_bag.host);
  error_rate = (total_requests == 0 ? 0.0 : ((double)error_count / (double)total_requests));
  GENERATE group.log_date AS log_date,
           group.log_hour AS log_hour,
           error_count AS error_request_count,
           total_requests AS total_request_count,
           error_rate AS error_rate,
           distinct_err_hosts AS distinct_error_hosts;
};
STORE q3 INTO '$OUTPUT/q3' USING PigStorage('\t');
