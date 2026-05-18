package com.example.multietl.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LogParserTest {
    @Test
    public void parsesValidLine() {
        String line = "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 200 786";
        LogParser parser = new LogParser();
        LogParser.ParseResult r = parser.parse(line);
        assertTrue(r.success);
        assertNotNull(r.record);
        assertEquals("199.72.81.55", r.record.getHost());
        assertEquals(200, r.record.getStatusCode());
        assertEquals(786L, r.record.getBytes());
        assertEquals("/images/NASA-logosmall.gif", r.record.getResourcePath());
    }

    @Test
    public void handlesMissingBytesAsZero() {
        String line = "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /a HTTP/1.0\" 404 -";
        LogParser parser = new LogParser();
        LogParser.ParseResult r = parser.parse(line);
        assertTrue(r.success);
        assertEquals(0L, r.record.getBytes());
    }

    @Test
    public void parsesRequestWithoutProtocol() {
        String line = "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /a\" 200 100";
        LogParser parser = new LogParser();
        LogParser.ParseResult r = parser.parse(line);
        assertTrue(r.success);
        assertEquals("/a", r.record.getResourcePath());
        assertNull(r.record.getProtocol());
    }

    @Test
    public void reportsMalformed() {
        String bad = "this is not a log line";
        LogParser parser = new LogParser();
        LogParser.ParseResult r = parser.parse(bad);
        assertFalse(r.success);
        assertTrue(r.record.isMalformed());
    }
}
