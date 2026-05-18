package com.example.multietl.pipelines.mapreduce.jobs;

import com.example.multietl.parser.LogRecord;

public class MapReduceLogRecord {
    private final String runId;
    private final long sequence;
    private final int ingestChunkId;
    private final LogRecord record;
    private final boolean parseSuccess;
    private final String parseError;
    private int batchId;

    public MapReduceLogRecord(String runId,
                              long sequence,
                              int ingestChunkId,
                              LogRecord record,
                              boolean parseSuccess,
                              String parseError,
                              int batchId) {
        this.runId = runId;
        this.sequence = sequence;
        this.ingestChunkId = ingestChunkId;
        this.record = record;
        this.parseSuccess = parseSuccess;
        this.parseError = parseError;
        this.batchId = batchId;
    }

    public String getRunId() {
        return runId;
    }

    public long getSequence() {
        return sequence;
    }

    public int getIngestChunkId() {
        return ingestChunkId;
    }

    public LogRecord getRecord() {
        return record;
    }

    public boolean isParseSuccess() {
        return parseSuccess;
    }

    public String getParseError() {
        return parseError;
    }

    public int getBatchId() {
        return batchId;
    }

    public void setBatchId(int batchId) {
        this.batchId = batchId;
    }
}
