package com.example.multietl.orchestrator;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

public class BatchManager {
    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    public interface BatchProcessor {
        void process(List<String> chunk) throws Exception;
    }

    public static void processFilesInChunks(List<Path> inputFiles, int chunkSize, BatchProcessor processor) throws Exception {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        List<String> chunk = new ArrayList<>(chunkSize);
        for (Path inputFile : inputFiles) {
            try (BufferedReader reader = openReader(inputFile)) {
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

    public static void processFilesByWeek(List<Path> inputFiles, BatchProcessor processor) throws Exception {
        List<String> chunk = new ArrayList<>();
        String currentWeek = null;

        for (Path inputFile : inputFiles) {
            try (BufferedReader reader = openReader(inputFile)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String week = extractWeekKey(line);
                    if (currentWeek == null) {
                        currentWeek = week != null ? week : "unknown";
                    }
                    if (week != null && !week.equals(currentWeek) && !chunk.isEmpty()) {
                        processor.process(chunk);
                        chunk = new ArrayList<>();
                        currentWeek = week;
                    }
                    chunk.add(line);
                }
            }
        }

        if (!chunk.isEmpty()) {
            processor.process(chunk);
        }
    }

    private static BufferedReader openReader(Path inputFile) throws Exception {
        InputStream in = Files.newInputStream(inputFile);
        if (inputFile.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".gz")) {
            in = new GZIPInputStream(in);
        }
        return new BufferedReader(new InputStreamReader(in, java.nio.charset.StandardCharsets.ISO_8859_1));
    }

    private static String extractWeekKey(String line) {
        if (line == null) {
            return null;
        }
        int lb = line.indexOf('[');
        int rb = line.indexOf(']');
        if (lb < 0 || rb <= lb) {
            return null;
        }
        try {
            OffsetDateTime timestamp = OffsetDateTime.parse(line.substring(lb + 1, rb), TS_FORMAT);
            WeekFields weeks = WeekFields.ISO;
            int year = timestamp.toLocalDate().get(weeks.weekBasedYear());
            int week = timestamp.toLocalDate().get(weeks.weekOfWeekBasedYear());
            return String.format("%04d-W%02d", year, week);
        } catch (Exception e) {
            return null;
        }
    }
}
