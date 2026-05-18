package com.example.multietl.pipelines.mapreduce.jobs;

public class SequencedLogLine {
    private final long sequence;
    private final String line;
    private final int ingestChunkId;

    public SequencedLogLine(long sequence, String line, int ingestChunkId) {
        this.sequence = sequence;
        this.line = line;
        this.ingestChunkId = ingestChunkId;
    }

    public long getSequence() {
        return sequence;
    }

    public String getLine() {
        return line;
    }

    public int getIngestChunkId() {
        return ingestChunkId;
    }
}
