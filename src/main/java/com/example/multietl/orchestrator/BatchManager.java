package com.example.multietl.orchestrator;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
            try (BufferedReader reader = Files.newBufferedReader(inputFile, java.nio.charset.StandardCharsets.ISO_8859_1)) {
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
}