package com.example.multietl.service;

import com.example.multietl.config.AppConfig;
import com.example.multietl.loader.DbLoader;
import com.example.multietl.orchestrator.Controller;
import com.example.multietl.service.EtlJobTracker.EtlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;
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

    public String submitJob(String pipeline, String inputFile, Integer batchSize) throws Exception {
        String jobId = UUID.randomUUID().toString();
        int actualBatchSize = batchSize != null ? batchSize : config.getBatchSize();
        
        Path inputPath = Path.of("data/raw", inputFile);
        if (!inputPath.toFile().exists()) {
            throw new IllegalArgumentException("Input file not found: " + inputPath);
        }

        EtlJob job = new EtlJob(jobId, pipeline, inputFile, actualBatchSize);
        jobTracker.addJob(jobId, job);

        executorService.submit(() -> runJob(jobId, pipeline, inputPath, actualBatchSize));
        
        return jobId;
    }

    private void runJob(String jobId, String pipeline, Path inputPath, int batchSize) {
        try {
            logger.info("Starting job {}: pipeline={}, input={}, batchSize={}", jobId, pipeline, inputPath, batchSize);
            
            var controller = new Controller(dbLoader);
            EtlJob job = jobTracker.getJob(jobId);
            Map<String, Object> finalMetrics = controller.run(pipeline, inputPath, batchSize, metrics -> {
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

    public Map<String, Object> getJobStatus(String jobId) {
        return jobTracker.getJobStatus(jobId);
    }

    public void shutdown() {
        executorService.shutdown();
    }
}
