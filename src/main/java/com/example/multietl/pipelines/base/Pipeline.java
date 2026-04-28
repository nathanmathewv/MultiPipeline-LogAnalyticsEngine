package com.example.multietl.pipelines.base;

import java.util.List;
import java.util.Map;

public interface Pipeline {
    void startRun(String runId);

    /**
     * Process a batch of raw log lines. batchId starts at 1.
     */
    void processBatch(List<String> rawLines, int batchId) throws Exception;

    /** Finalize and run queries; return query results keyed by query name. */
    Map<String, List<Map<String, Object>>> finalizeRun() throws Exception;

    void shutdown();

    /**
     * Return runtime metrics such as processed, malformed, total_batches
     */
    Map<String, Object> getMetrics();
}
