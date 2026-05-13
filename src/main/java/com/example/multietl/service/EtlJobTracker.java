package com.example.multietl.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EtlJobTracker {
    private static final EtlJobTracker instance = new EtlJobTracker();
    private final Map<String, EtlJob> jobs = new ConcurrentHashMap<>();

    private EtlJobTracker() {}

    public static EtlJobTracker getInstance() {
        return instance;
    }

    public void addJob(String jobId, EtlJob job) {
        jobs.put(jobId, job);
    }

    public EtlJob getJob(String jobId) {
        return jobs.get(jobId);
    }

    public Map<String, Object> getJobStatus(String jobId) {
        EtlJob job = jobs.get(jobId);
        if (job == null) {
            return Map.of("error", "Job not found");
        }
        return job.toMap();
    }

    public static class EtlJob {
        public String jobId;
        public String pipeline;
        public String inputFile;
        public String batchMode;
        public int batchSize;
        public int batchDays;
        public String status; // RUNNING, COMPLETED, FAILED
        public long startTime;
        public long endTime;
        public long totalRecords;
        public long malformedRecords;
        public String errorMessage;
        public Map<String, Object> results;

        public EtlJob(
                String jobId,
                String pipeline,
                String inputFile,
                String batchMode,
                int batchSize,
                int batchDays
        ){
            this.jobId = jobId;
            this.pipeline = pipeline;
            this.inputFile = inputFile;
            this.batchMode = batchMode;
            this.batchSize = batchSize;
            this.batchDays = batchDays;
            this.status = "RUNNING";
            this.startTime = System.currentTimeMillis();
        }

        public void markCompleted(long totalRecords, long malformedRecords, Map<String, Object> results) {
            this.status = "COMPLETED";
            this.endTime = System.currentTimeMillis();
            this.totalRecords = totalRecords;
            this.malformedRecords = malformedRecords;
            this.results = results;
        }

        public void markFailed(String errorMessage) {
            this.status = "FAILED";
            this.endTime = System.currentTimeMillis();
            this.errorMessage = errorMessage;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("jobId", jobId);
            map.put("pipeline", pipeline);
            map.put("inputFile", inputFile);
            map.put("batchMode", batchMode);
            map.put("batchSize", batchSize);
            map.put("batchDays", batchDays);
            map.put("status", status);
            map.put("startTime", startTime);
            if (endTime > 0) {
                map.put("endTime", endTime);
                map.put("runtimeMs", endTime - startTime);
            }
            map.put("totalRecords", totalRecords);
            map.put("malformedRecords", malformedRecords);
            if (errorMessage != null) {
                map.put("errorMessage", errorMessage);
            }
            return map;
        }
    }
}
