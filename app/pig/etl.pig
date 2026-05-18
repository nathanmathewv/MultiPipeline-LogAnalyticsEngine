%default INPUT ''
%default OUTPUT ''
%default BATCH_MODE days
%default BATCH_SIZE 1

raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);
ranked_raw = RANK raw;

parsed = FOREACH ranked_raw {
  record_seq = (long)$0;
  line = (chararray)$1;
  host = REGEX_EXTRACT(line, '^(\\S+) \\S+ \\S+ \\[(\\S+\\s-\\d{4})\\] "(\\S+) (\\S+)(?: (\\S+))?" (\\d{3}) (\\S+)', 1);
  ts = REGEX_EXTRACT(line, '^(\\S+) \\S+ \\S+ \\[(\\S+\\s-\\d{4})\\] "(\\S+) (\\S+)(?: (\\S+))?" (\\d{3}) (\\S+)', 2);
  method = REGEX_EXTRACT(line, '^(\\S+) \\S+ \\S+ \\[(\\S+\\s-\\d{4})\\] "(\\S+) (\\S+)(?: (\\S+))?" (\\d{3}) (\\S+)', 3);
  resource_path = REGEX_EXTRACT(line, '^(\\S+) \\S+ \\S+ \\[(\\S+\\s-\\d{4})\\] "(\\S+) (\\S+)(?: (\\S+))?" (\\d{3}) (\\S+)', 4);
  protocol = REGEX_EXTRACT(line, '^(\\S+) \\S+ \\S+ \\[(\\S+\\s-\\d{4})\\] "(\\S+) (\\S+)(?: (\\S+))?" (\\d{3}) (\\S+)', 5);
  status_str = REGEX_EXTRACT(line, '^(\\S+) \\S+ \\S+ \\[(\\S+\\s-\\d{4})\\] "(\\S+) (\\S+)(?: (\\S+))?" (\\d{3}) (\\S+)', 6);
  bytes_str = REGEX_EXTRACT(line, '^(\\S+) \\S+ \\S+ \\[(\\S+\\s-\\d{4})\\] "(\\S+) (\\S+)(?: (\\S+))?" (\\d{3}) (\\S+)', 7);
  bytes_ok = REGEX_EXTRACT(bytes_str, '^(\\d+|-)$', 1);
  ts_dt = (ts is null ? null : ToDate(ts, 'dd/MMM/yyyy:HH:mm:ss Z'));
  log_date = (ts_dt is null ? null : ToString(ts_dt, 'yyyy-MM-dd'));
  log_month = (log_date is null ? null : SUBSTRING(log_date, 0, 7));
  log_hour = (ts is null ? null : (int)SUBSTRING(ts, 12, 14));
  status_code = (status_str is null ? null : (int)status_str);
  bytes_val = (bytes_ok is null ? 0L : (bytes_ok == '-' ? 0L : (long)bytes_ok));
  malformed = ((host is not null AND ts_dt is not null AND method is not null AND resource_path is not null AND status_str is not null AND bytes_ok is not null) ? 0L : 1L);
  record_batch_id = (int)(((record_seq - 1L) / (long)$BATCH_SIZE) + 1L);
  GENERATE record_seq AS record_seq,
           record_batch_id AS record_batch_id,
           host AS host,
           log_date AS log_date,
           log_month AS log_month,
           log_hour AS log_hour,
           method AS method,
           resource_path AS resource_path,
           protocol AS protocol,
           status_code AS status_code,
           bytes_val AS bytes_val,
           malformed AS malformed;
};

parsed_with_date = FILTER parsed BY log_date is not null;
parsed_no_date = FILTER parsed BY log_date is null;

date_group = GROUP parsed_with_date BY log_date;
date_counts = FOREACH date_group GENERATE group AS log_date,
                                         COUNT(parsed_with_date) AS total_records,
                                         SUM(parsed_with_date.malformed) AS malformed_records;
ordered_dates = ORDER date_counts BY log_date ASC;
ranked_dates = RANK ordered_dates;
date_batches = FOREACH ranked_dates GENERATE (chararray)$1 AS log_date,
                                           (int)((($0 - 1L) / (long)$BATCH_SIZE) + 1L) AS day_batch_id;

joined = JOIN parsed_with_date BY log_date, date_batches BY log_date;
batched_with_date = FOREACH joined {
  selected_batch_id = ('$BATCH_MODE' == 'records' ? parsed_with_date::record_batch_id : date_batches::day_batch_id);
  GENERATE parsed_with_date::record_seq AS record_seq,
           selected_batch_id AS batch_id,
           parsed_with_date::host AS host,
           parsed_with_date::log_date AS log_date,
           parsed_with_date::log_month AS log_month,
           parsed_with_date::log_hour AS log_hour,
           parsed_with_date::method AS method,
           parsed_with_date::resource_path AS resource_path,
           parsed_with_date::protocol AS protocol,
           parsed_with_date::status_code AS status_code,
           parsed_with_date::bytes_val AS bytes_val,
           parsed_with_date::malformed AS malformed;
};

day_dated = FOREACH joined GENERATE parsed_with_date::record_seq AS record_seq,
                                   date_batches::day_batch_id AS day_batch_id;
day_batch_start_group = GROUP day_dated BY day_batch_id;
day_batch_starts = FOREACH day_batch_start_group GENERATE group AS day_batch_id,
                                                        MIN(day_dated.record_seq) AS start_seq;

no_date_candidate_cross = CROSS parsed_no_date, day_batch_starts;
no_date_candidates = FILTER no_date_candidate_cross BY day_batch_starts::start_seq <= parsed_no_date::record_seq;
no_date_candidate_pairs = FOREACH no_date_candidates GENERATE parsed_no_date::record_seq AS record_seq,
                                                            day_batch_starts::start_seq AS start_seq;
no_date_candidate_group = GROUP no_date_candidate_pairs BY record_seq;
no_date_best_start = FOREACH no_date_candidate_group GENERATE group AS record_seq,
                                                               MAX(no_date_candidate_pairs.start_seq) AS start_seq;
no_date_batch_lookup_join = JOIN no_date_best_start BY start_seq, day_batch_starts BY start_seq;
no_date_best_batches = FOREACH no_date_batch_lookup_join GENERATE no_date_best_start::record_seq AS record_seq,
                                                               day_batch_starts::day_batch_id AS day_batch_id;
no_date_joined = JOIN parsed_no_date BY record_seq LEFT OUTER, no_date_best_batches BY record_seq;
batched_no_date = FOREACH no_date_joined {
  selected_batch_id = ('$BATCH_MODE' == 'records' ? parsed_no_date::record_batch_id : (no_date_best_batches::day_batch_id is null ? 1 : no_date_best_batches::day_batch_id));
  GENERATE parsed_no_date::record_seq AS record_seq,
           selected_batch_id AS batch_id,
           parsed_no_date::host AS host,
           parsed_no_date::log_date AS log_date,
           parsed_no_date::log_month AS log_month,
           parsed_no_date::log_hour AS log_hour,
           parsed_no_date::method AS method,
           parsed_no_date::resource_path AS resource_path,
           parsed_no_date::protocol AS protocol,
           parsed_no_date::status_code AS status_code,
           parsed_no_date::bytes_val AS bytes_val,
           parsed_no_date::malformed AS malformed;
};

batched = UNION batched_with_date, batched_no_date;
valid_batches = FILTER batched BY batch_id > 0;
parsed_good = FILTER valid_batches BY malformed == 0;

summary_group = GROUP batched ALL;
summary = FOREACH summary_group GENERATE COUNT(batched) AS total_records,
                                         SUM(batched.malformed) AS malformed_records;
STORE summary INTO '$OUTPUT/malformed_summary' USING PigStorage('\t');

batch_group = GROUP valid_batches BY batch_id;
batch_counts = FOREACH batch_group GENERATE group AS batch_id,
                                          COUNT(valid_batches) AS total_records,
                                          SUM(valid_batches.malformed) AS malformed_records;
STORE batch_counts INTO '$OUTPUT/batch_counts' USING PigStorage('\t');

batch_date_group = GROUP valid_batches BY (batch_id, log_date);
batch_date_counts = FOREACH batch_date_group GENERATE group.batch_id AS batch_id,
                                                    group.log_date AS log_date,
                                                    COUNT(valid_batches) AS total_records,
                                                    SUM(valid_batches.malformed) AS malformed_records;
STORE batch_date_counts INTO '$OUTPUT/batch_date_counts' USING PigStorage('\t');

q1_group = GROUP parsed_good BY (batch_id, log_date, status_code);
q1 = FOREACH q1_group GENERATE group.batch_id AS batch_id,
                               group.log_date AS log_date,
                               group.status_code AS status_code,
                               COUNT(parsed_good) AS request_count,
                               SUM(parsed_good.bytes_val) AS total_bytes;
STORE q1 INTO '$OUTPUT/q1' USING PigStorage('\t');

q2_group = GROUP parsed_good BY (batch_id, log_month, resource_path);
q2_counts = FOREACH q2_group {
  host_bag = FOREACH parsed_good GENERATE host;
  unique_hosts = DISTINCT host_bag;
  GENERATE group.batch_id AS batch_id,
           group.log_month AS log_month,
           group.resource_path AS resource_path,
           COUNT(parsed_good) AS request_count,
           SUM(parsed_good.bytes_val) AS total_bytes,
           COUNT(unique_hosts) AS distinct_host_count;
};
q2_by_batch_month = GROUP q2_counts BY (batch_id, log_month);
q2_top = FOREACH q2_by_batch_month {
  ordered = ORDER q2_counts BY request_count DESC;
  limited = LIMIT ordered 20;
  GENERATE FLATTEN(limited);
};
STORE q2_top INTO '$OUTPUT/q2' USING PigStorage('\t');

errors = FOREACH parsed_good GENERATE batch_id,
                                      log_date,
                                      log_hour,
                                      host,
                                      (status_code >= 400 AND status_code <= 599 ? 1 : 0) AS is_error;
q3_group = GROUP errors BY (batch_id, log_date, log_hour);
q3 = FOREACH q3_group {
  total_requests = COUNT(errors);
  error_bag = FILTER errors BY is_error == 1;
  error_count = COUNT(error_bag);
  error_host_bag = FOREACH error_bag GENERATE host;
  unique_error_hosts = DISTINCT error_host_bag;
  distinct_err_hosts = COUNT(unique_error_hosts);
  error_rate = (total_requests == 0 ? 0.0 : ((double)error_count / (double)total_requests));
  GENERATE group.batch_id AS batch_id,
           group.log_date AS log_date,
           group.log_hour AS log_hour,
           error_count AS error_request_count,
           total_requests AS total_request_count,
           error_rate AS error_rate,
           distinct_err_hosts AS distinct_error_hosts;
};
STORE q3 INTO '$OUTPUT/q3' USING PigStorage('\t');
