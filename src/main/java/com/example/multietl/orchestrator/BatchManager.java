package com.example.multietl.orchestrator;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
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

    public static void processFilesByDays(
        List<Path> inputFiles,
        int numDays,
        BatchProcessor processor
        ) throws Exception {

            if (numDays <= 0) {
                throw new IllegalArgumentException("numDays must be > 0");
            }

            List<String> chunk = new ArrayList<>();

            Long currentBucket = null;

            for (Path inputFile : inputFiles) {

                try (BufferedReader reader = openReader(inputFile)) {

                    String line;

                    while ((line = reader.readLine()) != null) {

                        Long bucket = extractDayBucket(line, numDays);

                        if (currentBucket == null && bucket != null) {
                            currentBucket = bucket;
                        }

                        if (bucket != null
                                && !bucket.equals(currentBucket)
                                && !chunk.isEmpty()) {

                            processor.process(chunk);

                            chunk = new ArrayList<>();

                            currentBucket = bucket;
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

    private static Long extractDayBucket(
        String line,
        int numDays
        ) {

            if (line == null) {
                return null;
            }

            int lb = line.indexOf('[');
            int rb = line.indexOf(']');

            if (lb < 0 || rb <= lb) {
                return null;
            }

            try {

                OffsetDateTime timestamp =
                    OffsetDateTime.parse(
                        line.substring(lb + 1, rb),
                        TS_FORMAT
                    );

                long epochDay =
                    timestamp.toLocalDate().toEpochDay();

                return epochDay / numDays;

            } catch (Exception e) {
                return null;
            }
        }

}