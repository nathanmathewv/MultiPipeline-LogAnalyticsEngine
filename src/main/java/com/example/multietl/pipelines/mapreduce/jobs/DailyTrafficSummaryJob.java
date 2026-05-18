package com.example.multietl.pipelines.mapreduce.jobs;

import com.example.multietl.parser.LogRecord;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DailyTrafficSummaryJob extends LocalMapReduceJob<MapReduceLogRecord, DailyTrafficSummaryJob.Key, Long, Map<String, Object>> {
    @Override
    protected void map(MapReduceLogRecord input, KeyValueCollector<Key, Long> collector) {
        LogRecord record = input.getRecord();
        if (!record.isMalformed() && input.getBatchId() > 0) {
            collector.collect(new Key(input.getBatchId(), record.getLogDate(), record.getStatusCode()), record.getBytes());
        }
    }

    @Override
    protected void reduce(Key key, List<Long> values, OutputCollector<Map<String, Object>> collector) {
        long totalBytes = 0;
        for (Long value : values) {
            totalBytes += value;
        }

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("batch_id", key.batchId());
        row.put("log_date", key.logDate());
        row.put("status_code", key.statusCode());
        row.put("request_count", (long) values.size());
        row.put("total_bytes", totalBytes);
        collector.collect(row);
    }

    public record Key(int batchId, String logDate, int statusCode) {
    }
}
