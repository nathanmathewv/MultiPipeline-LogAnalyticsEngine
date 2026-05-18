package com.example.multietl.orchestrator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchManagerTest {
    @TempDir
    Path tempDir;

    @Test
    void readsGzipLogFilesAsTextLines() throws Exception {
        Path gz = tempDir.resolve("NASA_access_log_Jul95.gz");
        try (GZIPOutputStream gzip = new GZIPOutputStream(Files.newOutputStream(gz));
             OutputStreamWriter writer = new OutputStreamWriter(gzip, StandardCharsets.ISO_8859_1)) {
            writer.write("line-one\nline-two\nline-three\n");
        }

        List<List<String>> chunks = new ArrayList<>();
        BatchManager.processFilesInChunks(List.of(gz), 2, chunk -> chunks.add(new ArrayList<>(chunk)));

        assertEquals(2, chunks.size());
        assertEquals(List.of("line-one", "line-two"), chunks.get(0));
        assertEquals(List.of("line-three"), chunks.get(1));
    }
}
