package com.example.multietl.pipelines.mapreduce.jobs;

import java.util.List;

public class MalformedSummaryJob extends LocalMapReduceJob<MapReduceLogRecord, String, MapReduceLogRecord, MalformedSummaryJob.Summary> {
    @Override
    protected void map(MapReduceLogRecord input, KeyValueCollector<String, MapReduceLogRecord> collector) {
        collector.collect("all", input);
    }

    @Override
    protected void reduce(String key, List<MapReduceLogRecord> values, OutputCollector<Summary> collector) {
        long malformed = values.stream().filter(v -> v.getRecord().isMalformed()).count();
        collector.collect(new Summary(values.size(), malformed));
    }

    public record Summary(long totalRecords, long malformedRecords) {
    }
}
