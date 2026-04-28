package com.example.multietl.reporting;

import com.example.multietl.loader.DbLoader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Reporter {
    private final DbLoader dbLoader;

    public Reporter(DbLoader dbLoader) {
        this.dbLoader = dbLoader;
    }

    public void printRunSummary(String runId) {
        System.out.println("===========================================");
        System.out.println("        ETL RUN SUMMARY");
        System.out.println("===========================================");
        
        try (ResultSet rs = dbLoader.queryRunMetadata(runId)) {
            if (rs.next()) {
                System.out.println("Run ID:           " + rs.getString("run_id"));
                System.out.println("Pipeline:         " + rs.getString("pipeline_name"));
                System.out.println("Batch ID:         all");
                System.out.println("Batch size:       " + rs.getInt("batch_size"));
                System.out.println("Avg batch size:   " + String.format("%.2f", rs.getDouble("avg_batch_size")));
                System.out.println("Total records:    " + rs.getLong("total_records"));
                System.out.println("Malformed records: " + rs.getLong("malformed_records"));
                System.out.println("Total batches:    " + rs.getInt("total_batches"));
                System.out.println("Runtime (ms):     " + rs.getLong("runtime_ms"));
            } else {
                System.out.println("No run metadata found for " + runId);
                return;
            }
        } catch (SQLException e) {
            System.err.println("Failed to read run metadata: " + e.getMessage());
            return;
        }

        System.out.println("\n===========================================");
        System.out.println("        QUERY RESULTS");
        System.out.println("===========================================\n");

        try (ResultSet r2 = dbLoader.queryEtlResults(runId)) {
            Map<String, List<String>> resultsByQuery = new LinkedHashMap<>();
            while (r2.next()) {
                String queryName = r2.getString("query_name");
                String row = formatRow(queryName, r2);
                resultsByQuery.computeIfAbsent(queryName, k -> new ArrayList<>()).add(row);
            }

            for (Map.Entry<String, List<String>> e : resultsByQuery.entrySet()) {
                System.out.println("Query: " + e.getKey());
                System.out.println("-------------------------------------------");
                for (String row : e.getValue()) {
                    System.out.println(row);
                }
                System.out.println();
            }
        } catch (SQLException e) {
            System.err.println("Failed to read etl results: " + e.getMessage());
        }
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
}
