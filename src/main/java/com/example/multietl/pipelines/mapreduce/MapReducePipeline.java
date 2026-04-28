package com.example.multietl.pipelines.mapreduce;

import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.common.CommonPipeline;

import java.util.List;
import java.util.Map;

/**
 * MapReduce pipeline implementation.
 * 
 * In a full Hadoop deployment:
 * - Mapper would parse each log line and emit intermediate key-value pairs
 * - Combiner would perform local aggregations for efficiency
 * - Reducer would aggregate results globally
 * - Multiple MapReduce jobs would compute the three required queries
 * - Batching would be handled via HDFS partitioning with run_id and batch_id
 * 
 * For now, uses in-memory CommonPipeline with the same ETL logic as MapReduce would execute.
 */
public class MapReducePipeline implements Pipeline {
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
