package com.example.multietl.pipelines.mapreduce.jobs;

import com.example.multietl.parser.LogRecord;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HourlyErrorAnalysisJob extends LocalMapReduceJob<MapReduceLogRecord, HourlyErrorAnalysisJob.Key, HourlyErrorAnalysisJob.Value, Map<String, Object>> {
    @Override
    protected void map(MapReduceLogRecord input, KeyValueCollector<Key, Value> collector) {
        LogRecord record = input.getRecord();
        if (!record.isMalformed() && input.getBatchId() > 0) {
            boolean error = record.getStatusCode() >= 400 && record.getStatusCode() <= 599;
            collector.collect(
                new Key(input.getBatchId(), record.getLogDate(), record.getLogHour()),
                new Value(error, record.getHost())
            );
        }
    }

    @Override
    protected void reduce(Key key, List<Value> values, OutputCollector<Map<String, Object>> collector) {
        long errorCount = 0;
        Set<String> errorHosts = new HashSet<>();
        for (Value value : values) {
            if (value.error()) {
                errorCount++;
                errorHosts.add(value.host());
            }
        }

        long total = values.size();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("batch_id", key.batchId());
        row.put("log_date", key.logDate());
        row.put("log_hour", key.logHour());
        row.put("error_request_count", errorCount);
        row.put("total_request_count", total);
        row.put("error_rate", total == 0 ? 0.0 : (double) errorCount / total);
        row.put("distinct_error_hosts", (long) errorHosts.size());
        collector.collect(row);
    }

    public record Key(int batchId, String logDate, int logHour) {
    }

    public record Value(boolean error, String host) {
    }
}
