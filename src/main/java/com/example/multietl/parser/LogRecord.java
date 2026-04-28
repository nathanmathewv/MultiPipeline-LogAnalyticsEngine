package com.example.multietl.parser;

import org.bson.Document;

import java.time.OffsetDateTime;

public class LogRecord {
    private final String host;
    private final OffsetDateTime timestamp;
    private final String logDate; // yyyy-MM-dd
    private final int logHour; // 0-23
    private final String method;
    private final String resourcePath;
    private final String protocol;
    private final int statusCode;
    private final long bytes;
    private final boolean malformed;
    private final String rawLine;

    public LogRecord(String host, OffsetDateTime timestamp, String logDate, int logHour,
                     String method, String resourcePath, String protocol,
                     int statusCode, long bytes, boolean malformed, String rawLine) {
        this.host = host;
        this.timestamp = timestamp;
        this.logDate = logDate;
        this.logHour = logHour;
        this.method = method;
        this.resourcePath = resourcePath;
        this.protocol = protocol;
        this.statusCode = statusCode;
        this.bytes = bytes;
        this.malformed = malformed;
        this.rawLine = rawLine;
    }

    public String getHost() { return host; }
    public OffsetDateTime getTimestamp() { return timestamp; }
    public String getLogDate() { return logDate; }
    public int getLogHour() { return logHour; }
    public String getMethod() { return method; }
    public String getResourcePath() { return resourcePath; }
    public String getProtocol() { return protocol; }
    public int getStatusCode() { return statusCode; }
    public long getBytes() { return bytes; }
    public boolean isMalformed() { return malformed; }
    public String getRawLine() { return rawLine; }

    public Document toDocument(String runId, int batchId) {
        Document d = new Document();
        d.append("host", host);
        d.append("timestamp", timestamp == null ? null : timestamp.toString());
        d.append("log_date", logDate);
        d.append("log_hour", logHour);
        d.append("method", method);
        d.append("resource_path", resourcePath);
        d.append("protocol", protocol);
        d.append("status_code", statusCode);
        d.append("bytes", bytes);
        d.append("malformed", malformed);
        d.append("raw_line", rawLine);
        d.append("run_id", runId);
        d.append("batch_id", batchId);
        return d;
    }
}
