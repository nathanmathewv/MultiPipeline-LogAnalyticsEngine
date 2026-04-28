package com.example.multietl.parser;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {
    // Example: 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 200 786

    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    public static class ParseResult {
        public final boolean success;
        public final LogRecord record;
        public final String error;

        private ParseResult(boolean success, LogRecord record, String error) {
            this.success = success;
            this.record = record;
            this.error = error;
        }

        public static ParseResult ok(LogRecord r) { return new ParseResult(true, r, null); }
        public static ParseResult malformed(String rawLine, String reason) {
            LogRecord r = new LogRecord(null, null, null, 0, null, null, null, 0, 0, true, rawLine);
            return new ParseResult(false, r, reason);
        }
    }

    public ParseResult parse(String line) {
        if (line == null || line.isBlank()) return ParseResult.malformed(line, "empty");
        try {
            // host
            int firstSpace = line.indexOf(' ');
            if (firstSpace < 0) return ParseResult.malformed(line, "no_host");
            String host = line.substring(0, firstSpace);

            // timestamp between [ and ]
            int lb = line.indexOf('[');
            int rb = line.indexOf(']');
            if (lb < 0 || rb < 0 || rb <= lb) return ParseResult.malformed(line, "no_timestamp");
            String ts = line.substring(lb + 1, rb);

            // request between first " and next "
            int q1 = line.indexOf('"');
            int q2 = line.indexOf('"', q1 + 1);
            if (q1 < 0 || q2 < 0) return ParseResult.malformed(line, "no_request");
            String request = line.substring(q1 + 1, q2);
            String[] reqParts = request.split(" ");
            if (reqParts.length < 2) return ParseResult.malformed(line, "bad_request");
            String method = reqParts[0];
            String resource = reqParts[1];
            String protocol = reqParts.length > 2 ? reqParts[2] : null;

            // after q2, remaining tokens for status and bytes
            String after = line.substring(q2 + 1).trim();
            String[] tokens = after.split("\\s+");
            if (tokens.length < 2) return ParseResult.malformed(line, "no_status_bytes");
            int status = Integer.parseInt(tokens[0]);
            String bytesStr = tokens[1];
            long bytes = bytesStr.equals("-") ? 0L : Long.parseLong(bytesStr);

            OffsetDateTime odt = null;
            try {
                odt = OffsetDateTime.parse(ts, TS_FORMAT);
            } catch (DateTimeParseException ex) {
                try {
                    odt = OffsetDateTime.parse(ts.replaceFirst(" ", "+"), DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ssZ", Locale.ENGLISH));
                } catch (Exception ex2) {
                    odt = null;
                }
            }

            if (odt == null) {
                return ParseResult.malformed(line, "bad_timestamp");
            }

            String logDate = odt.toLocalDate().toString();
            int hour = odt.getHour();

            LogRecord record = new LogRecord(host, odt, logDate, hour, method, resource, protocol, status, bytes, false, line);
            return ParseResult.ok(record);
        } catch (Exception e) {
            return ParseResult.malformed(line, "exception:" + e.getMessage());
        }
    }
}
