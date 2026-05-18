package com.example.multietl.pipelines.pig;

import com.example.multietl.config.AppConfig;
import com.example.multietl.pipelines.base.BatchConfig;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

/**
 * Apache Pig pipeline implementation using a containerized Pig runner.
 * Parsing and aggregation are executed by Pig scripts, not in Java.
 */
public class PigPipeline implements Pipeline {
    private final AppConfig config;
    private String runId;
    private BatchConfig batchConfig = BatchConfig.days(1);
    private long rawLoaded = 0;
    private int ingestChunks = 0;
    private long processed = 0;
    private long malformed = 0;
    private Path runDir;
    private Path inputDir;
    private Path outputDir;
    private final Map<Integer, long[]> batchStats = new LinkedHashMap<>();
    private final Map<Integer, String[]> batchDateRanges = new LinkedHashMap<>();
    private List<Map<String, Object>> batchSummaries = new ArrayList<>();

    public PigPipeline() {
        this(null);
    }

    public PigPipeline(AppConfig config) {
        this.config = config;
    }

    @Override
    public void startRun(String runId, BatchConfig batchConfig) {
        this.runId = runId;
        this.batchConfig = batchConfig == null ? BatchConfig.days(1) : batchConfig;
        this.rawLoaded = 0;
        this.ingestChunks = 0;
        this.processed = 0;
        this.malformed = 0;
        this.batchStats.clear();
        this.batchDateRanges.clear();
        this.batchSummaries = new ArrayList<>();
        this.runDir = Path.of("results", "pig", "run_" + runId);
        this.inputDir = runDir.resolve("input");
        this.outputDir = runDir.resolve("output");
        try {
            Files.createDirectories(inputDir);
            Files.createDirectories(outputDir);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Pig run directories", e);
        }
    }

    @Override
    public void processBatch(List<String> rawLines, int chunkId) throws Exception {
        ingestChunks++;
        rawLoaded += rawLines.size();

        Path chunkFile = inputDir.resolve(String.format("chunk_%05d.log", chunkId));
        Files.write(chunkFile, rawLines, StandardCharsets.ISO_8859_1,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun(QueryPlan plan) throws Exception {
        ensurePigImage();
        clearDirectory(outputDir);
        runPigJob();

        Map<String, List<Map<String, Object>>> results = new LinkedHashMap<>();
        List<Map<String, Object>> q1Rows = readQueryRows("q1", 5);
        List<Map<String, Object>> q2Rows = readQueryRows("q2", 6);
        List<Map<String, Object>> q3Rows = readQueryRows("q3", 7);
        loadBatchStats();
        loadBatchDateRanges();
        loadMalformedSummary();

        batchSummaries = buildBatchSummaries();

        if (plan.isSplitByMonth()) {
            if (plan.getQueries().contains(QueryType.DAILY_TRAFFIC_SUMMARY)) {
                addScopedByMonth(results, QueryType.DAILY_TRAFFIC_SUMMARY, q1Rows, row -> (String) row.get("log_date"));
            }
            if (plan.getQueries().contains(QueryType.TOP_RESOURCES)) {
                addScopedByMonth(results, QueryType.TOP_RESOURCES, q2Rows, row -> (String) row.get("log_month"));
            }
            if (plan.getQueries().contains(QueryType.HOURLY_ERROR_ANALYSIS)) {
                addScopedByMonth(results, QueryType.HOURLY_ERROR_ANALYSIS, q3Rows, row -> (String) row.get("log_date"));
            }
        } else {
            if (plan.getQueries().contains(QueryType.DAILY_TRAFFIC_SUMMARY)) {
                results.put(QueryType.DAILY_TRAFFIC_SUMMARY.getKey(), q1Rows);
            }
            if (plan.getQueries().contains(QueryType.TOP_RESOURCES)) {
                results.put(QueryType.TOP_RESOURCES.getKey(), q2Rows);
            }
            if (plan.getQueries().contains(QueryType.HOURLY_ERROR_ANALYSIS)) {
                results.put(QueryType.HOURLY_ERROR_ANALYSIS.getKey(), q3Rows);
            }
        }

        return results;
    }

    @Override
    public void shutdown() {
        batchStats.clear();
        batchDateRanges.clear();
        batchSummaries = new ArrayList<>();
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("processed", processed);
        m.put("malformed", malformed);
        m.put("total_records", processed + malformed);
        m.put("total_batches", batchSummaries.size());
        m.put("batch_mode", batchConfig.getModeKey());
        m.put("batch_size", batchConfig.getSize());
        m.put("raw_loaded", rawLoaded);
        m.put("ingest_chunks", ingestChunks);
        return m;
    }

    @Override
    public List<Map<String, Object>> getBatchSummaries() {
        return batchSummaries;
    }

    private void ensurePigImage() throws Exception {
        String image = getPigImage();
        ProcessBuilder inspect = new ProcessBuilder("docker", "image", "inspect", image);
        Process inspectProc = inspect.start();
        int inspectExit = inspectProc.waitFor();
        if (inspectExit == 0) {
            return;
        }

        Path dockerfile = Path.of("app", "pig", "Dockerfile");
        if (!dockerfile.toFile().exists()) {
            throw new IllegalStateException("Pig Dockerfile not found at " + dockerfile);
        }

        ProcessBuilder build = new ProcessBuilder("docker", "build", "-t", image, "-f", dockerfile.toString(), ".");
        build.inheritIO();
        Process buildProc = build.start();
        int buildExit = buildProc.waitFor();
        if (buildExit != 0) {
            throw new RuntimeException("Failed to build Pig Docker image: " + image);
        }
    }

    private void runPigJob() throws Exception {
        String image = getPigImage();
        String scriptPath = normalizeContainerPath(getPigScriptPath());
        String workspace = Path.of("").toAbsolutePath().toString();
        String inputGlob = normalizeContainerPath(inputDir.toString()) + "/*.log";
        String outputPath = normalizeContainerPath(outputDir.toString());

        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        cmd.add("run");
        cmd.add("--rm");
        cmd.add("-v");
        cmd.add(workspace + ":/workspace");
        cmd.add("-w");
        cmd.add("/workspace");
        cmd.add(image);
        cmd.add("pig");
        cmd.add("-x");
        cmd.add("local");
        cmd.add("-f");
        cmd.add("/workspace/" + scriptPath);
        cmd.add("-param");
        cmd.add("INPUT=/workspace/" + inputGlob);
        cmd.add("-param");
        cmd.add("OUTPUT=/workspace/" + outputPath);
        cmd.add("-param");
        cmd.add("BATCH_MODE=" + batchConfig.getModeKey());
        cmd.add("-param");
        cmd.add("BATCH_SIZE=" + batchConfig.getSize());

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.inheritIO();
        Process proc = pb.start();
        int exit = proc.waitFor();
        if (exit != 0) {
            throw new RuntimeException("Pig job failed with exit code " + exit);
        }
    }

    private List<Map<String, Object>> readQueryRows(String folder, int expectedCols) throws Exception {
        Path dir = outputDir.resolve(folder);
        if (!dir.toFile().exists()) {
            return List.of();
        }
        List<Path> partFiles = Files.list(dir)
            .filter(p -> p.getFileName().toString().startsWith("part-"))
            .collect(Collectors.toList());
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Path file : partFiles) {
            try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    String[] parts = line.split("\t", -1);
                    if (parts.length < expectedCols) {
                        continue;
                    }
                    rows.add(buildRow(folder, parts));
                }
            }
        }
        return rows;
    }

    private Map<String, Object> buildRow(String folder, String[] parts) {
        Map<String, Object> row = new LinkedHashMap<>();
        switch (folder) {
            case "q1":
                row.put("batch_id", Integer.parseInt(parts[0]));
                row.put("log_date", parts[1]);
                row.put("status_code", Integer.parseInt(parts[2]));
                row.put("request_count", Long.parseLong(parts[3]));
                row.put("total_bytes", Double.parseDouble(parts[4]));
                break;
            case "q2":
                row.put("batch_id", Integer.parseInt(parts[0]));
                row.put("log_month", parts[1]);
                row.put("resource_path", parts[2]);
                row.put("request_count", Long.parseLong(parts[3]));
                row.put("total_bytes", Double.parseDouble(parts[4]));
                row.put("distinct_host_count", Long.parseLong(parts[5]));
                break;
            case "q3":
                row.put("batch_id", Integer.parseInt(parts[0]));
                row.put("log_date", parts[1]);
                row.put("log_hour", Integer.parseInt(parts[2]));
                row.put("error_request_count", Long.parseLong(parts[3]));
                row.put("total_request_count", Long.parseLong(parts[4]));
                row.put("error_rate", Double.parseDouble(parts[5]));
                row.put("distinct_error_hosts", Long.parseLong(parts[6]));
                break;
            default:
                break;
        }
        return row;
    }

    private void loadBatchStats() throws Exception {
        Path dir = outputDir.resolve("batch_counts");
        if (!dir.toFile().exists()) {
            return;
        }
        List<Path> partFiles = Files.list(dir)
            .filter(p -> p.getFileName().toString().startsWith("part-"))
            .collect(Collectors.toList());
        for (Path file : partFiles) {
            try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    String[] parts = line.split("\t", -1);
                    if (parts.length < 3) {
                        continue;
                    }
                    int batchId = Integer.parseInt(parts[0]);
                    long total = Long.parseLong(parts[1]);
                    long malformedCount = Long.parseLong(parts[2]);
                    batchStats.put(batchId, new long[] {total, malformedCount});
                }
            }
        }
    }

    private void loadBatchDateRanges() throws Exception {
        Path dir = outputDir.resolve("batch_date_counts");
        if (!dir.toFile().exists()) {
            return;
        }
        List<Path> partFiles = Files.list(dir)
            .filter(p -> p.getFileName().toString().startsWith("part-"))
            .collect(Collectors.toList());
        for (Path file : partFiles) {
            try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    String[] parts = line.split("\t", -1);
                    if (parts.length < 2 || parts[1].isBlank()) {
                        continue;
                    }
                    int batchId = Integer.parseInt(parts[0]);
                    String logDate = parts[1];
                    String[] range = batchDateRanges.computeIfAbsent(batchId, id -> new String[] {null, null});
                    if (range[0] == null || logDate.compareTo(range[0]) < 0) {
                        range[0] = logDate;
                    }
                    if (range[1] == null || logDate.compareTo(range[1]) > 0) {
                        range[1] = logDate;
                    }
                }
            }
        }
    }

    private void loadMalformedSummary() throws Exception {
        Path dir = outputDir.resolve("malformed_summary");
        if (!dir.toFile().exists()) {
            return;
        }
        List<Path> partFiles = Files.list(dir)
            .filter(p -> p.getFileName().toString().startsWith("part-"))
            .collect(Collectors.toList());
        for (Path file : partFiles) {
            try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    String[] parts = line.split("\t", -1);
                    if (parts.length < 2) {
                        continue;
                    }
                    long total = Long.parseLong(parts[0]);
                    long malformedCount = Long.parseLong(parts[1]);
                    malformed = malformedCount;
                    processed = total - malformedCount;
                    return;
                }
            }
        }
    }

    private List<Map<String, Object>> buildBatchSummaries() {
        Map<Integer, Map<String, Object>> summaries = new LinkedHashMap<>();
        for (Map.Entry<Integer, long[]> entry : batchStats.entrySet()) {
            int batchId = entry.getKey();
            long[] stats = entry.getValue();
            String[] range = batchDateRanges.getOrDefault(batchId, new String[] {null, null});
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("batch_id", batchId);
            summary.put("batch_start_date", range[0]);
            summary.put("batch_end_date", range[1]);
            putBatchConfig(summary);
            summary.put("records_total", stats[0]);
            summary.put("malformed_records", stats[1]);
            summaries.put(batchId, summary);
        }
        return new ArrayList<>(summaries.values());
    }

    private void putBatchConfig(Map<String, Object> m) {
        m.put("batch_mode", batchConfig.getModeKey());
        m.put("batch_size", batchConfig.getSize());
        m.put("batch_size_days", batchConfig.getBatchSizeDays());
        m.put("batch_size_records", batchConfig.getBatchSizeRecords());
    }

    private void addScopedByMonth(Map<String, List<Map<String, Object>>> results,
                                  QueryType type,
                                  List<Map<String, Object>> rows,
                                  java.util.function.Function<Map<String, Object>, String> monthExtractor) {
        for (Map<String, Object> row : rows) {
            String month = monthFromValue(monthExtractor.apply(row));
            if (month == null) {
                continue;
            }
            results.computeIfAbsent(scopedKey(type, month), k -> new ArrayList<>()).add(row);
        }
    }

    private String monthFromValue(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() >= 7) {
            return value.substring(0, 7);
        }
        return value;
    }

    private String scopedKey(QueryType type, String month) {
        return type.getKey() + "|month=" + month;
    }

    private String getPigImage() {
        return config != null ? config.getPigImage() : "multietl-pig:latest";
    }

    private String getPigScriptPath() {
        return config != null ? config.getPigScriptPath() : "app/pig/etl.pig";
    }

    private String normalizeContainerPath(String path) {
        return path.replace('\\', '/');
    }

    private void clearDirectory(Path dir) throws Exception {
        if (!dir.toFile().exists()) {
            Files.createDirectories(dir);
            return;
        }
        List<Path> paths = Files.walk(dir).sorted((a, b) -> b.compareTo(a)).collect(Collectors.toList());
        for (Path p : paths) {
            if (!p.equals(dir)) {
                Files.deleteIfExists(p);
            }
        }
    }
}
