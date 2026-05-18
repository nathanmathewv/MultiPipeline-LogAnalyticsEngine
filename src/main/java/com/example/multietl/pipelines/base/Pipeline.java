package com.example.multietl.pipelines.base;

import java.util.List;
import java.util.Map;

public interface Pipeline {
    void startRun(String runId, BatchConfig batchConfig);

    /**
     * Process an ingest chunk of raw log lines. chunkId starts at 1.
     */
    void processBatch(List<String> rawLines, int chunkId) throws Exception;

    /** Finalize and run queries; return query results keyed by query name. */
    Map<String, List<Map<String, Object>>> finalizeRun(QueryPlan plan) throws Exception;

    void shutdown();

    /**
     * Return runtime metrics such as processed, malformed, total_batches
     */
    Map<String, Object> getMetrics();

    /**
     * Return per-batch summaries keyed by batch_id and date range.
     */
    List<Map<String, Object>> getBatchSummaries();
}
