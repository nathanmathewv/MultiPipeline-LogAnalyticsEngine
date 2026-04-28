package com.example.multietl.orchestrator;

import com.example.multietl.loader.DbLoader;
import com.example.multietl.pipelines.Strategy.PipelineFactory;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.reporting.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Controller {
    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    private final DbLoader dbLoader;

    public Controller(DbLoader dbLoader) {
        this.dbLoader = dbLoader;
    }

    public Map<String, Object> run(String pipelineName, Path inputFile, int batchSize, java.util.function.Consumer<Map<String, Object>> metricsCallback) throws Exception {
        String runId = UUID.randomUUID().toString();
        Pipeline pipeline = PipelineFactory.create(pipelineName, null);
        pipeline.startRun(runId);

        Instant start = Instant.now();
        int[] batchIdRef = {1};
        BatchManager.processFileInBatches(inputFile, batchSize, batch -> {
            int batchId = batchIdRef[0]++;
            logger.info("Starting batch {} (size={})", batchId, batch.size());
            pipeline.processBatch(batch, batchId);
            logger.info("Finished batch {}", batchId);
            if (metricsCallback != null) {
                metricsCallback.accept(pipeline.getMetrics());
            }
        });

        Map<String, List<Map<String, Object>>> results = pipeline.finalizeRun();
        Instant end = Instant.now();
        long runtimeMs = Duration.between(start, end).toMillis();

        Map<String, Object> metrics = pipeline.getMetrics();
        int totalBatches = (int) metrics.getOrDefault("total_batches", 0);
        long totalRecords = ((Number) metrics.getOrDefault("total_records", 0)).longValue();
        long malformed = ((Number) metrics.getOrDefault("malformed", 0)).longValue();
        double avgBatchSize = totalBatches == 0 ? 0.0 : ((double) totalRecords) / totalBatches;

        // persist metadata and results
        dbLoader.insertRunMetadata(runId, pipelineName, batchSize, avgBatchSize, totalRecords, malformed, totalBatches, runtimeMs);
        dbLoader.insertEtlResults(runId, pipelineName, results);

        pipeline.shutdown();

        Reporter reporter = new Reporter(dbLoader);
        reporter.printRunSummary(runId);
        
        metrics.put("results", results);
        return metrics;
    }
}
