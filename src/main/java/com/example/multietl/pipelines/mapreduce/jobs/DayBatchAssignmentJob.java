package com.example.multietl.pipelines.mapreduce.jobs;

import com.example.multietl.pipelines.base.BatchConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class DayBatchAssignmentJob extends LocalMapReduceJob<MapReduceLogRecord, String, MapReduceLogRecord, MapReduceLogRecord> {
    private final BatchConfig batchConfig;
    private int dateIndex = 0;

    public DayBatchAssignmentJob(BatchConfig batchConfig) {
        this.batchConfig = batchConfig;
    }

    @Override
    public List<MapReduceLogRecord> run(List<MapReduceLogRecord> inputs) {
        TreeSet<String> dates = new TreeSet<>();
        for (MapReduceLogRecord input : inputs) {
            String logDate = input.getRecord().getLogDate();
            if (logDate != null) {
                dates.add(logDate);
            }
        }

        Map<String, Integer> dateToBatch = new HashMap<>();
        int dateIndex = 0;
        for (String date : dates) {
            dateToBatch.put(date, (dateIndex / batchConfig.getSize()) + 1);
            dateIndex++;
        }

        int fallbackBatchId = inputs.isEmpty() ? 0 : 1;
        int currentBatchId = fallbackBatchId;
        for (MapReduceLogRecord input : inputs) {
            String logDate = input.getRecord().getLogDate();
            if (logDate != null) {
                currentBatchId = dateToBatch.getOrDefault(logDate, currentBatchId);
            }
            input.setBatchId(currentBatchId);
        }
        return new ArrayList<>(inputs);
    }

    @Override
    protected Map<String, List<MapReduceLogRecord>> newGroupingMap() {
        return new TreeMap<>();
    }

    @Override
    protected void map(MapReduceLogRecord input, KeyValueCollector<String, MapReduceLogRecord> collector) {
        String logDate = input.getRecord().getLogDate();
        if (logDate != null) {
            collector.collect(logDate, input);
        } else {
            input.setBatchId(0);
        }
    }

    @Override
    protected void reduce(String key, List<MapReduceLogRecord> values, OutputCollector<MapReduceLogRecord> collector) {
        int batchId = (dateIndex / batchConfig.getSize()) + 1;
        dateIndex++;
        for (MapReduceLogRecord value : values) {
            value.setBatchId(batchId);
            collector.collect(value);
        }
    }
}
