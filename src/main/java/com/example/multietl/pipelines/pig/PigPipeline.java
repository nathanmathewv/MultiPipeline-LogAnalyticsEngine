package com.example.multietl.pipelines.pig;

import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.common.CommonPipeline;

import java.util.List;
import java.util.Map;

/**
 * Apache Pig pipeline implementation.
 * 
 * In a full Hadoop deployment:
 * - Raw logs would be written to HDFS
 * - Pig Latin scripts would parse and transform the data
 * - GROUP and FOREACH operations would compute aggregations
 * - Results would be written back to HDFS and read into etl_results
 * 
 * For now, uses in-memory CommonPipeline with the same ETL logic as Pig would execute.
 */
public class PigPipeline implements Pipeline {
    private final CommonPipeline delegate = new CommonPipeline();

    @Override
    public void startRun(String runId) {
        delegate.startRun(runId);
    }

    @Override
    public void processBatch(List<String> rawLines, int batchId) throws Exception {
        delegate.processBatch(rawLines, batchId);
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun() throws Exception {
        return delegate.finalizeRun();
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public Map<String, Object> getMetrics() {
        return delegate.getMetrics();
    }
}
