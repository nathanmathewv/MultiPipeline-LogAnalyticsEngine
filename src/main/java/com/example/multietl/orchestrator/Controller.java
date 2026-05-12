package com.example.multietl.orchestrator;

import com.example.multietl.loader.DbLoader;
import com.example.multietl.pipelines.Strategy.PipelineFactory;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.config.AppConfig;
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
    private final AppConfig config;

    public Controller(DbLoader dbLoader, AppConfig config) {
        this.dbLoader = dbLoader;
        this.config = config;
    }

    public Map<String, Object> run(String pipelineName,
                                  List<Path> inputFiles,
                                  int ingestChunkSize,
                                  int batchSize,
                                  QueryPlan queryPlan,
                                  java.util.function.Consumer<Map<String, Object>> metricsCallback) throws Exception {
        String runId = UUID.randomUUID().toString();
        Pipeline pipeline = PipelineFactory.create(pipelineName, config);
        pipeline.startRun(runId, batchSize);

        Instant start = Instant.now();
        int[] chunkIdRef = {1};
        BatchManager.BatchProcessor processor = chunk -> {
            int chunkId = chunkIdRef[0]++;
            logger.info("Starting ingest chunk {} (size={})", chunkId, chunk.size());
            pipeline.processBatch(chunk, chunkId);
            logger.info("Finished ingest chunk {}", chunkId);
            if (metricsCallback != null) {
                metricsCallback.accept(pipeline.getMetrics());
            }
        };

        if ("week".equalsIgnoreCase(config.getBatchMode()) || "weekly".equalsIgnoreCase(config.getBatchMode())) {
            logger.info("Using weekly record batching");
            BatchManager.processFilesByWeek(inputFiles, processor);
        } else {
            logger.info("Using fixed-size record batching with chunk size {}", ingestChunkSize);
            BatchManager.processFilesInChunks(inputFiles, ingestChunkSize, processor);
        }

        Map<String, List<Map<String, Object>>> results = pipeline.finalizeRun(queryPlan);
        Instant end = Instant.now();
        long runtimeMs = Duration.between(start, end).toMillis();

        Map<String, Object> metrics = pipeline.getMetrics();
        int totalBatches = (int) metrics.getOrDefault("total_batches", 0);
        long totalRecords = ((Number) metrics.getOrDefault("total_records", 0)).longValue();
        long malformed = ((Number) metrics.getOrDefault("malformed", 0)).longValue();
        double avgBatchSize = totalBatches == 0 ? 0.0 : ((double) totalRecords) / totalBatches;

        // persist metadata and results
        dbLoader.insertRunMetadata(runId, pipelineName, batchSize, avgBatchSize, totalRecords, malformed, totalBatches, runtimeMs);
        dbLoader.insertBatchMetadata(runId, pipelineName, batchSize, pipeline.getBatchSummaries());
        dbLoader.insertMalformedSummary(runId, pipelineName, totalRecords, malformed);
        dbLoader.insertEtlResults(runId, pipelineName, results);

        pipeline.shutdown();

        Reporter reporter = new Reporter(dbLoader);
        reporter.printRunSummary(runId, true, true);
        
        metrics.put("results", results);
        return metrics;
    }
}
