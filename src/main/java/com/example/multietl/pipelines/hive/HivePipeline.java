package com.example.multietl.pipelines.hive;

import com.example.multietl.pipelines.base.Pipeline;

import java.util.List;
import java.util.Map;

/**
 * Stub implementation for Apache Hive pipeline.
 * TODO: create external Hive table over raw logs or HDFS files, run SQL-like queries to compute aggregations.
 * Batching: load each batch into partitioned table with partition key run_id and batch_id.
 */
public class HivePipeline implements Pipeline {
    @Override
    public void startRun(String runId) {
        throw new UnsupportedOperationException("Hive pipeline not implemented");
    }

    @Override
    public void processBatch(List<String> rawLines, int batchId) {
        // TODO: write to HDFS and run LOAD or INSERT INTO partitioned table
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun() throws Exception {
        // TODO: run HiveQL queries for the defined reports and return results
        return Map.of();
    }

    @Override
    public void shutdown() {
        // TODO: cleanup
    }

    @Override
    public java.util.Map<String, Object> getMetrics() {
        return java.util.Map.of("processed", 0, "malformed", 0, "total_records", 0, "total_batches", 0);
    }
}
