package com.example.multietl.orchestrator;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class BatchManager {
    public interface BatchProcessor {
        void process(List<String> chunk) throws Exception;
    }

    public static void processFilesInChunks(List<Path> inputFiles, int chunkSize, BatchProcessor processor) throws Exception {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        List<String> chunk = new ArrayList<>(chunkSize);
        for (Path inputFile : inputFiles) {
            try (BufferedReader reader = openLogReader(inputFile)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    chunk.add(line);
                    if (chunk.size() == chunkSize) {
                        processor.process(chunk);
                        chunk.clear();
                    }
                }
            }
        }
        if (!chunk.isEmpty()) {
            processor.process(chunk);
        }
    }

    public static BufferedReader openLogReader(Path inputFile) throws Exception {
        InputStream stream = Files.newInputStream(inputFile);
        if (inputFile.getFileName().toString().toLowerCase().endsWith(".gz")) {
            stream = new GZIPInputStream(stream);
        }
        return new BufferedReader(new InputStreamReader(stream, java.nio.charset.StandardCharsets.ISO_8859_1));
    }
}
