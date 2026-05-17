package com.example.multietl.loader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbLoader {
    private static final Logger logger = LoggerFactory.getLogger(DbLoader.class);

    private final String url;
    private final String user;
    private final String password;

    public DbLoader(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    private Connection getConn() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public void insertRunMetadata(
        String runId,
        String pipelineName,
        String batchMode,
        int batchSize,
        int batchDays,
        double avgBatchSize,
        long totalRecords,
        long malformedRecords,
        int totalBatches,
        long runtimeMs
    ) throws SQLException {
        String sql =
                    "INSERT INTO run_metadata(" +
                    "run_id, pipeline_name, batch_mode, batch_records, batch_days," +
                    "avg_batch_size, total_records, malformed_records," +
                    "total_batches, runtime_ms, created_at" +
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,now())";
        try (Connection c = getConn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, runId);
            ps.setString(2, pipelineName);
            ps.setString(3, batchMode);
            ps.setInt(4, batchSize);
            ps.setInt(5, batchDays);
            ps.setDouble(6, avgBatchSize);
            ps.setLong(7, totalRecords);
            ps.setLong(8, malformedRecords);
            ps.setInt(9, totalBatches);
            ps.setLong(10, runtimeMs);
            ps.executeUpdate();
            logger.info("Inserted run_metadata for {}", runId);
        }
    }

    public void insertEtlResults(String runId, String pipelineName, Map<String, List<Map<String, Object>>> results) throws SQLException {
        String sql = "INSERT INTO etl_results(run_id,pipeline_name,batch_id,query_name,scope,k1,k2,m1,m2,m3,m4,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,now())";
        try (Connection c = getConn(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (Map.Entry<String, List<Map<String, Object>>> e : results.entrySet()) {
                String queryName = e.getKey();
                String scope = null;
                String baseName = queryName;
                int scopeIdx = queryName.indexOf("|month=");
                if (scopeIdx >= 0) {
                    baseName = queryName.substring(0, scopeIdx);
                    scope = queryName.substring(scopeIdx + 7);
                }
                for (Map<String, Object> row : e.getValue()) {
                    ps.setString(1, runId);
                    ps.setString(2, pipelineName);
                    ps.setObject(3, row.getOrDefault("batch_id", 0));
                    ps.setString(4, baseName);
                    ps.setString(5, scope);
                    ps.setObject(6, row.getOrDefault("k1", row.getOrDefault("log_date", row.getOrDefault("resource_path", null))));
                    ps.setObject(7, row.getOrDefault("k2", row.getOrDefault("status_code", row.getOrDefault("log_hour", null))));
                    ps.setObject(8, row.getOrDefault("m1", row.getOrDefault("request_count", row.getOrDefault("error_request_count", null))));
                    ps.setObject(9, row.getOrDefault("m2", row.getOrDefault("total_bytes", row.getOrDefault("total_request_count", null))));
                    ps.setObject(10, row.getOrDefault("m3", row.getOrDefault("distinct_host_count", row.getOrDefault("distinct_error_hosts", null))));
                    ps.setObject(11, row.getOrDefault("m4", row.getOrDefault("error_rate", null)));
                    ps.addBatch();
                }
            }
            ps.executeBatch();
            logger.info("Inserted etl_results for run {}", runId);
        }
    }

    public void insertBatchMetadata(String runId, String pipelineName, String batchMode, int batchSizeRecords, int batchDays, List<Map<String, Object>> summaries) throws SQLException {
        if (summaries == null || summaries.isEmpty()) {
            return;
        }
        String sql =
                    "INSERT INTO batch_metadata(" +
                    "run_id, pipeline_name, batch_id, batch_start_date, batch_end_date," +
                    "batch_mode, batch_records, batch_days," +
                    "records_total, malformed_records, created_at" +
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,now())";
        try (Connection c = getConn(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (Map<String, Object> summary : summaries) {
                ps.setString(1, runId);
                ps.setString(2, pipelineName);
                ps.setObject(3, summary.getOrDefault("batch_id", 0));
                ps.setObject(4, summary.getOrDefault("batch_start_date", null));
                ps.setObject(5, summary.getOrDefault("batch_end_date", null));
                ps.setString(6, batchMode);
                ps.setInt(7, batchSizeRecords);
                ps.setInt(8, batchDays);
                ps.setObject(9, summary.getOrDefault("records_total", null));
                ps.setObject(10, summary.getOrDefault("malformed_records", null));
                ps.addBatch();
            }
            ps.executeBatch();
            logger.info("Inserted batch_metadata for run {}", runId);
        }
    }

    public void insertMalformedSummary(String runId, String pipelineName, long totalRecords, long malformedRecords) throws SQLException {
        String sql = "INSERT INTO malformed_summary(run_id,pipeline_name,total_records,malformed_records,created_at) VALUES(?,?,?,?,now())";
        try (Connection c = getConn(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, runId);
            ps.setString(2, pipelineName);
            ps.setLong(3, totalRecords);
            ps.setLong(4, malformedRecords);
            ps.executeUpdate();
            logger.info("Inserted malformed_summary for run {}", runId);
        }
    }

    public ResultSet queryRunMetadata(String runId) throws SQLException {
        Connection c = getConn();
        String sql = "SELECT * FROM run_metadata WHERE run_id = ?";
        PreparedStatement ps = c.prepareStatement(sql);
        ps.setString(1, runId);
        return ps.executeQuery();
    }

    public ResultSet queryEtlResults(String runId) throws SQLException {
        Connection c = getConn();
        String sql = "SELECT * FROM etl_results WHERE run_id = ? ORDER BY query_name, scope";
        PreparedStatement ps = c.prepareStatement(sql);
        ps.setString(1, runId);
        return ps.executeQuery();
    }

    public ResultSet queryBatchMetadata(String runId) throws SQLException {
        Connection c = getConn();
        String sql = "SELECT * FROM batch_metadata WHERE run_id = ? ORDER BY batch_id";
        PreparedStatement ps = c.prepareStatement(sql);
        ps.setString(1, runId);
        return ps.executeQuery();
    }

    public ResultSet queryMalformedSummary(String runId) throws SQLException {
        Connection c = getConn();
        String sql = "SELECT * FROM malformed_summary WHERE run_id = ?";
        PreparedStatement ps = c.prepareStatement(sql);
        ps.setString(1, runId);
        return ps.executeQuery();
    }
}
