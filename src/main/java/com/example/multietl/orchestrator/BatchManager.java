package com.example.multietl.orchestrator;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BatchManager {
    public interface BatchProcessor {
        void process(List<String> batch) throws Exception;
    }

    public static void processFileInBatches(Path inputFile, int batchSize, BatchProcessor processor) throws Exception {
        try (BufferedReader reader = Files.newBufferedReader(inputFile, java.nio.charset.StandardCharsets.ISO_8859_1)) {
            List<String> batch = new ArrayList<>(batchSize);
            String line;
            while ((line = reader.readLine()) != null) {
                batch.add(line);
                if (batch.size() == batchSize) {
                    processor.process(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                processor.process(batch);
            }
        }
    }

    public static List<List<String>> split(List<String> lines, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < lines.size(); i += batchSize) {
            int end = Math.min(lines.size(), i + batchSize);
            batches.add(new ArrayList<>(lines.subList(i, end)));
        }
        return batches;
    }
}
