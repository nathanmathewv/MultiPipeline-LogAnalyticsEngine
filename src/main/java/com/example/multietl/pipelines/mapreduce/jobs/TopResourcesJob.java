package com.example.multietl.pipelines.mapreduce.jobs;

import com.example.multietl.parser.LogRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TopResourcesJob extends LocalMapReduceJob<MapReduceLogRecord, TopResourcesJob.Key, TopResourcesJob.Value, Map<String, Object>> {
    @Override
    public List<Map<String, Object>> run(List<MapReduceLogRecord> inputs) {
        List<Map<String, Object>> rows = super.run(inputs);
        rows.sort(Comparator
            .comparingInt((Map<String, Object> row) -> ((Number) row.get("batch_id")).intValue())
            .thenComparing((a, b) -> Long.compare(
                ((Number) b.get("request_count")).longValue(),
                ((Number) a.get("request_count")).longValue()
            ))
            .thenComparing(row -> String.valueOf(row.get("resource_path"))));

        Map<Integer, Integer> rowsPerBatch = new LinkedHashMap<>();
        List<Map<String, Object>> topRows = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            int batchId = ((Number) row.get("batch_id")).intValue();
            int currentCount = rowsPerBatch.getOrDefault(batchId, 0);
            if (currentCount < 20) {
                topRows.add(row);
                rowsPerBatch.put(batchId, currentCount + 1);
            }
        }
        return topRows;
    }

    @Override
    protected void map(MapReduceLogRecord input, KeyValueCollector<Key, Value> collector) {
        LogRecord record = input.getRecord();
        if (!record.isMalformed() && input.getBatchId() > 0) {
            collector.collect(
                new Key(input.getBatchId(), record.getResourcePath()),
                new Value(record.getBytes(), record.getHost())
            );
        }
    }

    @Override
    protected void reduce(Key key, List<Value> values, OutputCollector<Map<String, Object>> collector) {
        long totalBytes = 0;
        Set<String> hosts = new HashSet<>();
        for (Value value : values) {
            totalBytes += value.bytes();
            hosts.add(value.host());
        }

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("batch_id", key.batchId());
        row.put("resource_path", key.resourcePath());
        row.put("request_count", (long) values.size());
        row.put("total_bytes", totalBytes);
        row.put("distinct_host_count", (long) hosts.size());
        collector.collect(row);
    }

    public record Key(int batchId, String resourcePath) {
    }

    public record Value(long bytes, String host) {
    }
}
