package com.example.multietl.reporting;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.example.multietl.loader.DbLoader;

public class Reporter {
    private final DbLoader dbLoader;

    public Reporter(DbLoader dbLoader) {
        this.dbLoader = dbLoader;
    }

    public void printRunSummary(String runId) {
        printRunSummary(runId, true, false);
    }

    public void printRunSummary(String runId, boolean printToConsole, boolean writeToFile) {
        String content = buildRunSummary(runId);
        if (printToConsole) {
            System.out.print(content);
        }
        if (writeToFile) {
            writeResultsFile(runId, content);
        }
    }

    private String buildRunSummary(String runId) {
        StringBuilder sb = new StringBuilder();
        appendLine(sb, "===========================================");
        appendLine(sb, "        ETL RUN SUMMARY");
        appendLine(sb, "===========================================");

        try (ResultSet rs = dbLoader.queryRunMetadata(runId)) {
            if (rs.next()) {
                appendLine(sb, "Run ID:           " + rs.getString("run_id"));
                appendLine(sb, "Pipeline:         " + rs.getString("pipeline_name"));
                appendLine(sb, "Batch ID:         all");
                appendLine(sb, "Batch mode:       " + rs.getString("batch_mode"));
                String batchMode = rs.getString("batch_mode");
                if ("records".equalsIgnoreCase(batchMode)) {
                    appendLine(sb, "Batch size:       " + rs.getInt("batch_size"));
                } else if ("days".equalsIgnoreCase(batchMode)) {
                    appendLine(sb, "Batch days:       " + rs.getInt("batch_days"));
                }
                appendLine(sb, "Avg batch size:   " + String.format("%.2f", rs.getDouble("avg_batch_size")));
                appendLine(sb, "Total records:    " + rs.getLong("total_records"));
                appendLine(sb, "Malformed records: " + rs.getLong("malformed_records"));
                appendLine(sb, "Total batches:    " + rs.getInt("total_batches"));
                appendLine(sb, "Runtime (ms):     " + rs.getLong("runtime_ms"));
            } else {
                appendLine(sb, "No run metadata found for " + runId);
                return sb.toString();
            }
        } catch (SQLException e) {
            appendLine(sb, "Failed to read run metadata: " + e.getMessage());
            return sb.toString();
        }

        appendLine(sb, "\n===========================================");
        appendLine(sb, "        BATCH METADATA");
        appendLine(sb, "===========================================");

        try (ResultSet rs = dbLoader.queryBatchMetadata(runId)) {
            boolean any = false;
            while (rs.next()) {
                any = true;
                appendLine(sb, String.format("Batch %s: %s to %s | Records: %s | Malformed: %s | Configured batch size: %s",
                    rs.getObject("batch_id"),
                    rs.getString("batch_start_date"),
                    rs.getString("batch_end_date"),
                    rs.getObject("records_total"),
                    rs.getObject("malformed_records"),
                    rs.getObject("batch_size_records")));
            }
            if (!any) {
                appendLine(sb, "No batch metadata found.");
            }
        } catch (SQLException e) {
            appendLine(sb, "Failed to read batch metadata: " + e.getMessage());
        }

        appendLine(sb, "\n===========================================");
        appendLine(sb, "        MALFORMED SUMMARY");
        appendLine(sb, "===========================================");

        try (ResultSet rs = dbLoader.queryMalformedSummary(runId)) {
            if (rs.next()) {
                appendLine(sb, "Total records:    " + rs.getLong("total_records"));
                appendLine(sb, "Malformed records: " + rs.getLong("malformed_records"));
            } else {
                appendLine(sb, "No malformed summary found.");
            }
        } catch (SQLException e) {
            appendLine(sb, "Failed to read malformed summary: " + e.getMessage());
        }

        appendLine(sb, "\n===========================================");
        appendLine(sb, "        QUERY RESULTS");
        appendLine(sb, "===========================================\n");

        try (ResultSet r2 = dbLoader.queryEtlResults(runId)) {
            Map<String, Map<String, List<ReportRow>>> resultsByScope = new LinkedHashMap<>();
            while (r2.next()) {
                String scope = r2.getString("scope");
                String scopeKey = scope == null ? "all" : scope;
                String queryName = r2.getString("query_name");
                ReportRow row = new ReportRow(
                    formatRow(queryName, r2),
                    r2.getString("k1"),
                    r2.getString("k2"),
                    r2.getDouble("m1"));
                resultsByScope
                    .computeIfAbsent(scopeKey, k -> new LinkedHashMap<>())
                    .computeIfAbsent(queryName, k -> new ArrayList<>())
                    .add(row);
            }

            for (Map.Entry<String, Map<String, List<ReportRow>>> scopeEntry : resultsByScope.entrySet()) {
                appendLine(sb, "Scope: " + scopeEntry.getKey());
                appendLine(sb, "-------------------------------------------");
                for (Map.Entry<String, List<ReportRow>> queryEntry : scopeEntry.getValue().entrySet()) {
                    sortRows(queryEntry.getKey(), queryEntry.getValue());
                    appendLine(sb, "Query: " + queryEntry.getKey());
                    for (ReportRow row : queryEntry.getValue()) {
                        appendLine(sb, row.text());
                    }
                    appendLine(sb, "");
                }
            }
        } catch (SQLException e) {
            appendLine(sb, "Failed to read etl results: " + e.getMessage());
        }

        return sb.toString();
    }

    private String formatRow(String queryName, ResultSet rs) throws SQLException {
        if (queryName.contains("daily_traffic")) {
            return String.format("  Date: %-10s | Status: %-3s | Requests: %-8s | Bytes: %s",
                    rs.getString("k1"), rs.getString("k2"), rs.getObject("m1"), rs.getObject("m2"));
        } else if (queryName.contains("top_resources")) {
            return String.format("  Resource: %-30s | Requests: %-8s | Bytes: %-10s | Distinct Hosts: %s",
                    rs.getString("k1"), rs.getObject("m1"), rs.getObject("m2"), rs.getObject("m3"));
        } else if (queryName.contains("hourly_error")) {
            return String.format("  Date: %-10s | Hour: %-2s | Errors: %-6s | Total: %-6s | Rate: %-6s | Distinct Error Hosts: %s",
                rs.getString("k1"), rs.getString("k2"), rs.getObject("m1"), rs.getObject("m2"), rs.getObject("m4"), rs.getObject("m3"));
        }
        return String.format("  k1=%s k2=%s m1=%s m2=%s m3=%s m4=%s",
                rs.getObject("k1"), rs.getObject("k2"), rs.getObject("m1"), rs.getObject("m2"), rs.getObject("m3"), rs.getObject("m4"));
    }

    private void sortRows(String queryName, List<ReportRow> rows) {
        if (queryName.contains("top_resources")) {
            rows.sort(Comparator
                .comparingDouble(ReportRow::metricOne).reversed()
                .thenComparing(row -> valueOrEmpty(row.k1())));
            return;
        }

        if (queryName.contains("daily_traffic") || queryName.contains("hourly_error")) {
            rows.sort(Comparator
                .comparing((ReportRow row) -> valueOrEmpty(row.k1()))
                .thenComparingInt(row -> numericSortKey(row.k2())));
        }
    }

    private String valueOrEmpty(String value) {
        return value == null ? "" : value;
    }

    private int numericSortKey(String value) {
        if (value == null || value.isBlank()) {
            return Integer.MAX_VALUE;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return Integer.MAX_VALUE;
        }
    }

    private record ReportRow(String text, String k1, String k2, double metricOne) {
    }

    private void appendLine(StringBuilder sb, String line) {
        sb.append(line).append("\n");
    }

    private void writeResultsFile(String runId, String content) {
        try {
            Path dir = Path.of("results");
            Files.createDirectories(dir);
            String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            Path file = dir.resolve("etl_run_" + runId + "_" + ts + ".log");
            Files.writeString(file, content, StandardCharsets.UTF_8);
        } catch (Exception e) {
            System.err.println("Failed to write results log: " + e.getMessage());
        }
    }
}
