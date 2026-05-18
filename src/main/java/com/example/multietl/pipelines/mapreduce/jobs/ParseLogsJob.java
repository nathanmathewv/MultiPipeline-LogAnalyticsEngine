package com.example.multietl.pipelines.mapreduce.jobs;

import com.example.multietl.parser.LogParser;
import com.example.multietl.pipelines.base.BatchConfig;

import java.util.List;

public class ParseLogsJob extends LocalMapReduceJob<SequencedLogLine, Long, MapReduceLogRecord, MapReduceLogRecord> {
    private final String runId;
    private final BatchConfig batchConfig;
    private final LogParser parser = new LogParser();

    public ParseLogsJob(String runId, BatchConfig batchConfig) {
        this.runId = runId;
        this.batchConfig = batchConfig;
    }

    @Override
    protected void map(SequencedLogLine input, KeyValueCollector<Long, MapReduceLogRecord> collector) {
        LogParser.ParseResult result = parser.parse(input.getLine());
        int batchId = batchConfig.isRecords() ? batchIdForRecord(input.getSequence()) : 0;
        collector.collect(input.getSequence(), new MapReduceLogRecord(
            runId,
            input.getSequence(),
            input.getIngestChunkId(),
            result.record,
            result.success,
            result.error,
            batchId
        ));
    }

    @Override
    protected void reduce(Long key, List<MapReduceLogRecord> values, OutputCollector<MapReduceLogRecord> collector) {
        for (MapReduceLogRecord value : values) {
            collector.collect(value);
        }
    }

    private int batchIdForRecord(long sequence) {
        return (int) (((sequence - 1) / batchConfig.getSize()) + 1);
    }
}
