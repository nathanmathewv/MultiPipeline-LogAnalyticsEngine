package com.example.multietl.util;

import com.example.multietl.orchestrator.BatchManager;

import java.io.BufferedReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class DatasetFiles {
    private static final String JULY = "NASA_access_log_Jul95";
    private static final String AUGUST = "NASA_access_log_Aug95";

    private DatasetFiles() {
    }

    public static List<Path> resolveDefaultDatasets(String dataDir) {
        List<Path> files = new ArrayList<>();
        addFirstExisting(files, dataDir, JULY);
        addFirstExisting(files, dataDir, AUGUST);
        return files;
    }

    public static List<Path> resolveInputFiles(String inputFile, String dataDir) {
        if (inputFile == null || inputFile.isBlank() || inputFile.equalsIgnoreCase("all")) {
            return resolveDefaultDatasets(dataDir);
        }

        Path requested = Path.of(inputFile);
        if (requested.isAbsolute() && requested.toFile().exists()) {
            return List.of(requested);
        }

        List<Path> candidates = new ArrayList<>();
        candidates.add(Path.of(dataDir, inputFile));
        if (!inputFile.toLowerCase().endsWith(".gz")) {
            candidates.add(Path.of(dataDir, inputFile + ".gz"));
        }
        candidates.add(requested);
        if (!requested.getFileName().toString().toLowerCase().endsWith(".gz")) {
            candidates.add(Path.of(inputFile + ".gz"));
        }

        for (Path candidate : candidates) {
            if (candidate.toFile().exists()) {
                return List.of(candidate);
            }
        }
        return List.of();
    }

    public static boolean isSupportedLogFile(String name) {
        String lower = name.toLowerCase();
        return lower.endsWith(".log")
            || lower.endsWith(".gz")
            || name.equals(JULY)
            || name.equals(AUGUST);
    }

    public static long countLines(Path file) throws Exception {
        long count = 0;
        try (BufferedReader reader = BatchManager.openLogReader(file)) {
            while (reader.readLine() != null) {
                count++;
            }
        }
        return count;
    }

    private static void addFirstExisting(List<Path> files, String dataDir, String baseName) {
        Path plain = Path.of(dataDir, baseName);
        Path gz = Path.of(dataDir, baseName + ".gz");
        if (plain.toFile().exists()) {
            files.add(plain);
        } else if (gz.toFile().exists()) {
            files.add(gz);
        }
    }
}
