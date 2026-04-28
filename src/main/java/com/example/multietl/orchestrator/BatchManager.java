package com.example.multietl.orchestrator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BatchManager {
    public static List<List<String>> splitFile(Path inputFile, int batchSize) throws IOException {
        List<String> lines = Files.readAllLines(inputFile);
        return split(lines, batchSize);
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
