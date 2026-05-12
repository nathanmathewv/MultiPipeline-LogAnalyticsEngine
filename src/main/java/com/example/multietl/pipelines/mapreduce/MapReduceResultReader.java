package com.example.multietl.pipelines.mapreduce;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MapReduceResultReader {
    public List<String> readPartLines(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return List.of();
        }

        List<Path> partFiles;
        try (var stream = Files.list(dir)) {
            partFiles = stream
                .filter(path -> path.getFileName().toString().startsWith("part-"))
                .sorted()
                .collect(Collectors.toList());
        }

        List<String> lines = new ArrayList<>();
        for (Path file : partFiles) {
            try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isBlank()) {
                        lines.add(line);
                    }
                }
            }
        }
        return lines;
    }
}
