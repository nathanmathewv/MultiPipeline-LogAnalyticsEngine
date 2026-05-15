package com.example.multietl.pipelines.pig;

import java.io.BufferedReader;
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

import com.example.multietl.config.AppConfig;
import com.example.multietl.pipelines.base.Pipeline;
import com.example.multietl.pipelines.base.QueryPlan;
import com.example.multietl.pipelines.base.QueryType;

/**
 * Apache Pig pipeline implementation using a containerized Pig runner.
 * Parsing and aggregation are executed by Pig scripts, not in Java.
 */
public class PigPipeline implements Pipeline {
    private final AppConfig config;
    private String runId;
    private int batchSizeRecords = 1;
    private long rawLoaded = 0;
    private int ingestChunks = 0;
    private long processed = 0;
    private long malformed = 0;
    private Path runDir;
    private Path inputDir;
    private Path outputDir;
    private List<Map<String, Object>> batchSummaries = new ArrayList<>();

    public PigPipeline() {
        this(null);
    }

    public PigPipeline(AppConfig config) {
        this.config = config;
    }

    @Override
    public void startRun(String runId, int batchSizeRecords) {
        this.runId = runId;
        this.batchSizeRecords = Math.max(1, batchSizeRecords);
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

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("batch_id", chunkId);
        summary.put("batch_start_date", null);
        summary.put("batch_end_date", null);
        summary.put("batch_size_records", batchSizeRecords);
        summary.put("records_total", (long) rawLines.size());
        summary.put("malformed_records", 0L);
        batchSummaries.add(summary);
    }

    @Override
    public Map<String, List<Map<String, Object>>> finalizeRun(QueryPlan plan) throws Exception {
        ensurePigImage();
        clearDirectory(outputDir);
        runPigJob();

        Map<String, List<Map<String, Object>>> results = new LinkedHashMap<>();
        List<Map<String, Object>> q1Rows = readQueryRows("q1", 4);
        List<Map<String, Object>> q2Rows = readQueryRows("q2", 4);
        List<Map<String, Object>> q3Rows = readQueryRows("q3", 6);
        loadMalformedSummary();

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
        batchSummaries = new ArrayList<>();
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("processed", processed);
        m.put("malformed", malformed);
        m.put("total_records", processed + malformed);
        m.put("total_batches", batchSummaries.size());
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
        String scriptPath = getPigScriptPath();
        String workspace = Path.of("").toAbsolutePath().toString();
        String inputGlob = "/workspace/" + inputDir.toString().replace("\\", "/");
String outputPath = "/workspace/" + outputDir.toString().replace("\\", "/");

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
        cmd.add("INPUT=" + inputGlob);        
        cmd.add("-param");
        cmd.add("OUTPUT=" + outputPath);
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
        row.put("batch_id", 0);
        switch (folder) {
            case "q1":
                row.put("log_date", parts[0]);
                row.put("status_code", Integer.parseInt(parts[1]));
                row.put("request_count", Long.parseLong(parts[2]));
                row.put("total_bytes", Double.parseDouble(parts[3]));
                break;
            case "q2":
                if (parts.length >= 5) {
                    row.put("log_month", parts[0]);
                    row.put("resource_path", parts[1]);
                    row.put("request_count", Long.parseLong(parts[2]));
                    row.put("total_bytes", Double.parseDouble(parts[3]));
                    row.put("distinct_host_count", Long.parseLong(parts[4]));
                } else {
                    row.put("resource_path", parts[0]);
                    row.put("request_count", Long.parseLong(parts[1]));
                    row.put("total_bytes", Double.parseDouble(parts[2]));
                    row.put("distinct_host_count", Long.parseLong(parts[3]));
                }
                break;
            case "q3":
                row.put("log_date", parts[0]);
                row.put("log_hour", Integer.parseInt(parts[1]));
                row.put("error_request_count", Long.parseLong(parts[2]));
                row.put("total_request_count", Long.parseLong(parts[3]));
                row.put("error_rate", Double.parseDouble(parts[4]));
                row.put("distinct_error_hosts", Long.parseLong(parts[5]));
                break;
            default:
                break;
        }
        return row;
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
