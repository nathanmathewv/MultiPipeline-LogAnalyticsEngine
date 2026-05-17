package com.example.multietl.service;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.multietl.config.AppConfig;
import com.example.multietl.loader.DbLoader;
import com.example.multietl.orchestrator.Controller;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.service.EtlJobTracker.EtlJob;

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

    public String submitJob(
        String pipeline,
        String inputFile,
        String batchMode,
        Integer batchSizeRecords,
        Integer batchDays
        ) throws Exception {
        String jobId = UUID.randomUUID().toString();
        int actualBatchSizeRecords =
        batchSizeRecords != null
                ? batchSizeRecords
                : config.getBatchSize();

        int actualBatchDays =
                batchDays != null
                        ? batchDays
                : config.getBatchDays();

        int ingestChunkSize =
        config.getIngestChunkSize();
        
        List<Path> inputPaths = resolveInputFiles(inputFile, config.getDataDir());
        if (inputPaths.isEmpty()) {
            throw new IllegalArgumentException("Input file(s) not found for: " + inputFile);
        }

        EtlJob job = new EtlJob(
            jobId,
            pipeline,
            inputFile,
            batchMode,
            actualBatchSizeRecords,
            actualBatchDays
        );
        jobTracker.addJob(jobId, job);

        QueryPlan queryPlan = QueryPlan.all(false);
        executorService.submit(() -> runJob(jobId, pipeline, inputPaths, batchMode, ingestChunkSize, actualBatchSizeRecords, actualBatchDays, queryPlan));
        
        return jobId;
    }

    private void runJob(String jobId,
                        String pipeline,
                        List<Path> inputPaths,
                        String batchMode,
                        int ingestChunkSize,
                        int batchSizeRecords,
                        int batchDays,
                        QueryPlan queryPlan) {
        try {
            logger.info(
                "Starting job {}: pipeline={}, input={}, batchMode={}, batchSizeRecords={}, batchDays={}",
                jobId,
                pipeline,
                inputPaths,
                batchMode,
                batchSizeRecords,
                batchDays
            );
            
            config.setBatchMode(batchMode);
            config.setBatchDays(batchDays);
            
            var controller = new Controller(dbLoader, config);
            EtlJob job = jobTracker.getJob(jobId);
            Map<String, Object> finalMetrics = controller.run(pipeline, inputPaths, ingestChunkSize, batchSizeRecords, queryPlan, metrics -> {
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

    private List<Path> resolveInputFiles(String inputFile, String dataDir) {
        if (inputFile == null || inputFile.isBlank() || inputFile.equalsIgnoreCase("all")) {
            List<Path> files = new java.util.ArrayList<>();
            addFirstExisting(files, dataDir, "NASA_access_log_Jul95", "NASA_access_log_Jul95.log", "NASA_access_log_Jul95.gz");
            addFirstExisting(files, dataDir, "NASA_access_log_Aug95", "NASA_access_log_Aug95.log", "NASA_access_log_Aug95.gz");
            return files;
        }

        Path inputPath = Path.of(dataDir, inputFile);
        if (inputPath.toFile().exists()) {
            return List.of(inputPath);
        }
        return List.of();
    }

    private void addFirstExisting(List<Path> files, String dataDir, String... candidates) {
        for (String candidate : candidates) {
            Path path = Path.of(dataDir, candidate);
            if (path.toFile().exists()) {
                files.add(path);
                return;
            }
        }
    }
}
