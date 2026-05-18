package com.example.multietl.pipelines.mapreduce.jobs;

import com.example.multietl.pipelines.base.BatchConfig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BatchMetadataJob extends LocalMapReduceJob<MapReduceLogRecord, Integer, MapReduceLogRecord, Map<String, Object>> {
    private final BatchConfig batchConfig;

    public BatchMetadataJob(BatchConfig batchConfig) {
        this.batchConfig = batchConfig;
    }

    @Override
    protected void map(MapReduceLogRecord input, KeyValueCollector<Integer, MapReduceLogRecord> collector) {
        if (input.getBatchId() > 0) {
            collector.collect(input.getBatchId(), input);
        }
    }

    @Override
    protected void reduce(Integer key, List<MapReduceLogRecord> values, OutputCollector<Map<String, Object>> collector) {
        long malformed = 0;
        String startDate = null;
        String endDate = null;
        for (MapReduceLogRecord value : values) {
            if (value.getRecord().isMalformed()) {
                malformed++;
            }
            String logDate = value.getRecord().getLogDate();
            if (logDate != null) {
                if (startDate == null || logDate.compareTo(startDate) < 0) {
                    startDate = logDate;
                }
                if (endDate == null || logDate.compareTo(endDate) > 0) {
                    endDate = logDate;
                }
            }
        }

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("batch_id", key);
        row.put("batch_start_date", startDate);
        row.put("batch_end_date", endDate);
        row.put("batch_mode", batchConfig.getModeKey());
        row.put("batch_size", batchConfig.getSize());
        row.put("batch_size_days", batchConfig.getBatchSizeDays());
        row.put("batch_size_records", batchConfig.getBatchSizeRecords());
        row.put("records_total", (long) values.size());
        row.put("malformed_records", malformed);
        collector.collect(row);
    }
}
