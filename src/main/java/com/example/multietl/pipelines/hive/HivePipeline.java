package com.example.multietl.pipelines.hive;

import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.common.CommonPipeline;

import java.util.List;
import java.util.Map;

/**
 * Apache Hive pipeline implementation.
 * 
 * In a full Hadoop deployment:
 * - External Hive table would be created over raw logs in HDFS
 * - Each batch would be loaded via INSERT or LOAD commands
 * - Table would be partitioned by run_id and batch_id
 * - HiveQL queries would compute the three required aggregations
 * - Results would be collected and written back to etl_results
 * 
 * For now, uses in-memory CommonPipeline with the same ETL logic as Hive would execute.
 */
public class HivePipeline implements Pipeline {
    private final CommonPipeline delegate = new CommonPipeline();

    @Override
    public void startRun(String runId, int batchSizeDays) {
        delegate.startRun(runId, batchSizeDays);
    }

    @Override
    public void processBatch(List<String> rawLines, int batchId) throws Exception {
        delegate.processBatch(rawLines, batchId);
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun(QueryPlan plan) throws Exception {
        return delegate.finalizeRun(plan);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public Map<String, Object> getMetrics() {
        return delegate.getMetrics();
    }

    @Override
    public List<Map<String, Object>> getBatchSummaries() {
        return delegate.getBatchSummaries();
    }
}
