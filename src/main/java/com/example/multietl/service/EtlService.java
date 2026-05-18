package com.example.multietl.service;

import com.example.multietl.config.AppConfig;
import com.example.multietl.loader.DbLoader;
import com.example.multietl.orchestrator.Controller;
import com.example.multietl.pipelines.base.BatchConfig;
import com.example.multietl.pipelines.base.BatchMode;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.service.EtlJobTracker.EtlJob;
import com.example.multietl.util.DatasetFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EtlService {
    private static final Logger logger = LoggerFactory.getLogger(EtlService.class);
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);
    private final AppConfig config;
    private final DbLoader dbLoader;
    private final EtlJobTracker jobTracker;

    public EtlService(AppConfig config, DbLoader dbLoader) {
        this.config = config;
        this.dbLoader = dbLoader;
        this.jobTracker = EtlJobTracker.getInstance();
    }

    public String submitJob(String pipeline, String inputFile, BatchConfig batchConfig) throws Exception {
        String jobId = UUID.randomUUID().toString();
        BatchConfig actualBatchConfig = batchConfig != null ? batchConfig : defaultBatchConfig();
        int ingestChunkSize = config.getIngestChunkSize();
        
        List<Path> inputPaths = resolveInputFiles(inputFile, config.getDataDir());
        if (inputPaths.isEmpty()) {
            throw new IllegalArgumentException("Input file(s) not found for: " + inputFile);
        }

        EtlJob job = new EtlJob(jobId, pipeline, inputFile, actualBatchConfig);
        jobTracker.addJob(jobId, job);

        QueryPlan queryPlan = QueryPlan.all(true);
        executorService.submit(() -> runJob(jobId, pipeline, inputPaths, ingestChunkSize, actualBatchConfig, queryPlan));
        
        return jobId;
    }

    private void runJob(String jobId,
                        String pipeline,
                        List<Path> inputPaths,
                        int ingestChunkSize,
                        BatchConfig batchConfig,
                        QueryPlan queryPlan) {
        try {
            logger.info("Starting job {}: pipeline={}, input={}, batch={}", jobId, pipeline, inputPaths, batchConfig.describe());

            var controller = new Controller(dbLoader, config);
            EtlJob job = jobTracker.getJob(jobId);
            Map<String, Object> finalMetrics = controller.run(pipeline, inputPaths, ingestChunkSize, batchConfig, queryPlan, metrics -> {
                if (job != null) {
                    job.totalRecords = ((Number) metrics.getOrDefault("processed", 0L)).longValue() + ((Number) metrics.getOrDefault("malformed", 0L)).longValue();
                    job.malformedRecords = ((Number) metrics.getOrDefault("malformed", 0L)).longValue();
                }
            });
            
            if (job != null) {
                long total = ((Number) finalMetrics.getOrDefault("total_records", 0L)).longValue();
                long malformed = ((Number) finalMetrics.getOrDefault("malformed", 0L)).longValue();
                
                @SuppressWarnings("unchecked")
                Map<String, Object> results = (Map<String, Object>) finalMetrics.getOrDefault("results", Map.of());
                
                job.markCompleted(total, malformed, results);
                logger.info("Job {} completed successfully", jobId);
            }
        } catch (Throwable e) {
            logger.error("Job {} failed", jobId, e);
            EtlJob job = jobTracker.getJob(jobId);
            if (job != null) {
                job.markFailed(e.getMessage());
            }
        }
    }

    public BatchConfig buildBatchConfig(String batchMode, Integer batchSize, Integer batchDays) {
        if (batchMode == null || batchMode.isBlank()) {
            if (batchDays != null) {
                return BatchConfig.days(batchDays);
            }
            if (batchSize != null) {
                return BatchConfig.records(batchSize);
            }
            return defaultBatchConfig();
        }

        BatchMode mode = BatchMode.from(batchMode);
        int size;
        if (mode == BatchMode.DAYS) {
            size = batchDays != null ? batchDays : (batchSize != null ? batchSize : config.getBatchSizeDays());
        } else {
            size = batchSize != null ? batchSize : config.getBatchSize();
        }
        return new BatchConfig(mode, size);
    }

    private BatchConfig defaultBatchConfig() {
        BatchMode mode = BatchMode.from(config.getBatchMode());
        int size = mode == BatchMode.DAYS ? config.getBatchSizeDays() : config.getBatchSize();
        return new BatchConfig(mode, size);
    }

    public Map<String, Object> getJobStatus(String jobId) {
        return jobTracker.getJobStatus(jobId);
    }

    public void shutdown() {
        executorService.shutdown();
    }

    private List<Path> resolveInputFiles(String inputFile, String dataDir) {
        return DatasetFiles.resolveInputFiles(inputFile, dataDir);
    }
}
