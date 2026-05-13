package com.example.multietl.pipelines.mapreduce;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.multietl.config.AppConfig;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;

/**
 * Docker-backed Hadoop MapReduce pipeline.
 *
 * The Java application stays on the host, writes one raw input file per record batch, then
 * runs a Docker image that executes Hadoop MapReduce jobs in local mode.
 */
public class MapReducePipeline implements Pipeline {
    private static final Logger logger = LoggerFactory.getLogger(MapReducePipeline.class);

    private final AppConfig config;
    private String runId;
    private int batchSizeRecords;
    private long rawLoaded;
    private long processed;
    private long malformed;
    private int totalBatches;
    private Path runDir;
    private Path inputDir;
    private Path outputDir;
    private final MapReduceResultReader resultReader = new MapReduceResultReader();
    private final List<Map<String, Object>> batchSummaries = new ArrayList<>();

    public MapReducePipeline() {
        this(null);
    }

    public MapReducePipeline(AppConfig config) {
        this.config = config;
    }

    @Override
    public void startRun(String runId, int batchSizeRecords) {
        this.runId = runId;
        this.batchSizeRecords = Math.max(1, batchSizeRecords);
        this.rawLoaded = 0;
        this.processed = 0;
        this.malformed = 0;
        this.totalBatches = 0;
        this.batchSummaries.clear();

        this.runDir = Path.of(getWorkDir(), "run_" + runId);
        this.inputDir = runDir.resolve("input");
        this.outputDir = runDir.resolve("output");

        try {
            Files.createDirectories(inputDir);
            Files.createDirectories(outputDir);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create MapReduce run directories", e);
        }

        logger.info("MapReducePipeline started run {} with batchSizeRecords={}", runId, this.batchSizeRecords);
    }

    @Override
    public void processBatch(List<String> rawLines, int chunkId) throws Exception {
        if (rawLines == null || rawLines.isEmpty()) {
            return;
        }

        Path batchFile = inputDir.resolve(String.format("batch_%05d.log", chunkId));
        Files.write(batchFile, rawLines, StandardCharsets.ISO_8859_1,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        rawLoaded += rawLines.size();
        totalBatches++;

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("batch_id", chunkId);
        summary.put("batch_start_date", null);
        summary.put("batch_end_date", null);
        summary.put("batch_size_records", batchSizeRecords);
        summary.put("records_total", (long) rawLines.size());
        summary.put("malformed_records", 0L);
        batchSummaries.add(summary);

        logger.info("Wrote MapReduce input batch {} with {} records to {}", chunkId, rawLines.size(), batchFile);
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun(QueryPlan plan) throws Exception {
        ensureDockerImage();
        clearDirectory(outputDir);
        runDockerMapReduce(plan);

        Map<String, List<Map<String, Object>>> results = readResults(plan);
        loadMalformedSummary();
        logger.info("Finalized MapReduce run {}: total_records={}, malformed={}", runId, rawLoaded, malformed);
        return results;
    }

    @Override
    public void shutdown() {
        // Keep run files under results/mapreduce for debugging and demonstration.
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("processed", processed);
        m.put("malformed", malformed);
        m.put("total_records", rawLoaded);
        m.put("total_batches", totalBatches);
        m.put("raw_loaded", rawLoaded);
        m.put("ingest_chunks", totalBatches);
        return m;
    }

    @Override
    public List<Map<String, Object>> getBatchSummaries() {
        return batchSummaries;
    }

    private void ensureDockerImage() throws Exception {
        String image = getImage();
        Process inspect = new ProcessBuilder("docker", "image", "inspect", image)
            .inheritIO()
            .start();
        if (inspect.waitFor() == 0) {
            return;
        }

        String dockerfile = getDockerfile();
        logger.info("Building MapReduce Docker image {} from {}", image, dockerfile);
        Process build = new ProcessBuilder("docker", "build", "-t", image, "-f", dockerfile, ".")
            .inheritIO()
            .start();
        int exit = build.waitFor();
        if (exit != 0) {
            throw new RuntimeException("Failed to build MapReduce Docker image " + image + " (exit " + exit + ")");
        }
    }

    private void runDockerMapReduce(QueryPlan plan) throws Exception {
        String workspace = Path.of("").toAbsolutePath().toString();
        String inputPath = inputDir.toString().replace('\\', '/');
        String outputPath = outputDir.toString().replace('\\', '/');

        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        cmd.add("run");
        cmd.add("--rm");
        cmd.add("-v");
        cmd.add(workspace + ":/workspace");
        cmd.add("-w");
        cmd.add("/workspace");
        cmd.add(getImage());
        cmd.add(getScriptPath());
        cmd.add(runId);
        cmd.add(inputPath);
        cmd.add(outputPath);
        cmd.add(toQueryNames(plan));
        cmd.add(Boolean.toString(plan.isSplitByMonth()));

        logger.info("Running Dockerized MapReduce job for run {}", runId);
        Process proc = new ProcessBuilder(cmd).inheritIO().start();
        int exit = proc.waitFor();
        if (exit != 0) {
            throw new RuntimeException("Dockerized MapReduce job failed with exit code " + exit);
        }
    }

    private Map<String, List<Map<String, Object>>> readResults(QueryPlan plan) throws Exception {
        Map<String, List<Map<String, Object>>> results = new LinkedHashMap<>();

        if (plan.getQueries().contains(QueryType.DAILY_TRAFFIC_SUMMARY)) {
            addQueryResults(results, QueryType.DAILY_TRAFFIC_SUMMARY, readDailyTrafficRows(), plan.isSplitByMonth(), "log_date");
        }
        if (plan.getQueries().contains(QueryType.TOP_RESOURCES)) {
            addQueryResults(results, QueryType.TOP_RESOURCES, readTopResourceRows(), plan.isSplitByMonth(), "log_month");
        }
        if (plan.getQueries().contains(QueryType.HOURLY_ERROR_ANALYSIS)) {
            addQueryResults(results, QueryType.HOURLY_ERROR_ANALYSIS, readHourlyErrorRows(), plan.isSplitByMonth(), "log_date");
        }

        return results;
    }

    private void addQueryResults(Map<String, List<Map<String, Object>>> results,
                                 QueryType type,
                                 List<Map<String, Object>> rows,
                                 boolean splitByMonth,
                                 String monthField) {
        if (!splitByMonth) {
            results.put(type.getKey(), rows);
            return;
        }

        for (Map<String, Object> row : rows) {
            String month = monthFromValue((String) row.get(monthField));
            if (month == null) {
                continue;
            }
            results.computeIfAbsent(type.getKey() + "|month=" + month, k -> new ArrayList<>()).add(row);
        }
    }

    private List<Map<String, Object>> readDailyTrafficRows() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String line : readPartLines(outputDir.resolve("q1"))) {
            String[] p = line.split("\\t", -1);
            if (p.length < 4) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("batch_id", 0);
            row.put("log_date", p[0]);
            row.put("status_code", Integer.parseInt(p[1]));
            row.put("request_count", Long.parseLong(p[2]));
            row.put("total_bytes", Double.parseDouble(p[3]));
            rows.add(row);
        }
        return rows;
    }

    private List<Map<String, Object>> readTopResourceRows() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String line : readPartLines(outputDir.resolve("q2"))) {
            String[] p = line.split("\\t", -1);
            if (p.length < 4) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("batch_id", 0);
            if (p.length >= 5) {
                row.put("log_month", p[0]);
                row.put("resource_path", p[1]);
                row.put("request_count", Long.parseLong(p[2]));
                row.put("total_bytes", Double.parseDouble(p[3]));
                row.put("distinct_host_count", Long.parseLong(p[4]));
            } else {
                row.put("resource_path", p[0]);
                row.put("request_count", Long.parseLong(p[1]));
                row.put("total_bytes", Double.parseDouble(p[2]));
                row.put("distinct_host_count", Long.parseLong(p[3]));
            }
            rows.add(row);
        }
        return rows;
    }

    private List<Map<String, Object>> readHourlyErrorRows() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String line : readPartLines(outputDir.resolve("q3"))) {
            String[] p = line.split("\\t", -1);
            if (p.length < 6) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("batch_id", 0);
            row.put("log_date", p[0]);
            row.put("log_hour", Integer.parseInt(p[1]));
            row.put("error_request_count", Long.parseLong(p[2]));
            row.put("total_request_count", Long.parseLong(p[3]));
            row.put("error_rate", Double.parseDouble(p[4]));
            row.put("distinct_error_hosts", Long.parseLong(p[5]));
            rows.add(row);
        }
        return rows;
    }

    private void loadMalformedSummary() throws Exception {
        long total = rawLoaded;
        long malformedRecords = 0L;
        List<String> lines = readPartLines(outputDir.resolve("malformed_summary"));
        if (!lines.isEmpty()) {
            String[] p = lines.get(0).split("\\t", -1);
            if (p.length >= 2) {
                total = Long.parseLong(p[0]);
                malformedRecords = Long.parseLong(p[1]);
            }
        }
        malformed = malformedRecords;
        processed = Math.max(0L, total - malformedRecords);
    }

    private List<String> readPartLines(Path dir) throws Exception {
        return resultReader.readPartLines(dir);
    }

    private void clearDirectory(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
            return;
        }
        List<Path> paths;
        try (var stream = Files.walk(dir)) {
            paths = stream.sorted((a, b) -> b.compareTo(a)).collect(Collectors.toList());
        }
        for (Path path : paths) {
            if (!path.equals(dir)) {
                Files.deleteIfExists(path);
            }
        }
    }

    private String toQueryNames(QueryPlan plan) {
        if (plan.getQueries().size() == QueryType.values().length) {
            return "all";
        }
        return plan.getQueries().stream()
            .map(QueryType::getKey)
            .collect(Collectors.joining(","));
    }

    private String monthFromValue(String value) {
        if (value == null || value.length() < 7) {
            return null;
        }
        return value.substring(0, 7);
    }

    private String getImage() {
        return config != null ? config.getMapReduceImage() : "multietl-mapreduce:latest";
    }

    private String getWorkDir() {
        return config != null ? config.getMapReduceWorkDir() : "results/mapreduce";
    }

    private String getDockerfile() {
        return config != null ? config.getMapReduceDockerfile() : "app/mapreduce/Dockerfile";
    }

    private String getScriptPath() {
        return config != null ? config.getMapReduceScriptPath() : "/usr/local/bin/run-mapreduce.sh";
    }
}
