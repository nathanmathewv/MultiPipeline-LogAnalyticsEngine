package com.example.multietl.api;

import com.example.multietl.service.EtlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.util.*;

@RestController
@RequestMapping("/api/etl")
@CrossOrigin(origins = "*")
public class EtlController {
    private static final Logger logger = LoggerFactory.getLogger(EtlController.class);
    
    private final EtlService etlService;

    @Autowired
    public EtlController(EtlService etlService) {
        this.etlService = etlService;
    }

    @PostMapping("/run")
    public ResponseEntity<?> startJob(
            @RequestParam String pipeline,
            @RequestParam String file,
            @RequestParam(required = false) Integer batchDays) {
        try {
            logger.info("Received ETL job request: pipeline={}, file={}, batchDays={}", pipeline, file, batchDays);
            String jobId = etlService.submitJob(pipeline, file, batchDays);
            return ResponseEntity.ok(Map.of(
                    "jobId", jobId,
                    "message", "ETL job submitted successfully",
                    "status", "RUNNING"
            ));
        } catch (Exception e) {
            logger.error("Failed to submit job", e);
            return ResponseEntity.badRequest().body(Map.of(
                    "error", e.getMessage()
            ));
        }
    }

    @GetMapping("/status/{jobId}")
    public ResponseEntity<?> getStatus(@PathVariable String jobId) {
        Map<String, Object> status = etlService.getJobStatus(jobId);
        if (status.containsKey("error")) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(status);
    }

    @GetMapping("/dataset/files")
    public ResponseEntity<?> listFiles() {
        try {
            File dataDir = new File("data/raw");
            if (!dataDir.exists()) {
                return ResponseEntity.ok(Map.of("files", new String[0]));
            }

            File[] files = dataDir.listFiles((d, name) -> 
                    name.endsWith(".log") || name.endsWith(".gz"));
            
            List<Map<String, Object>> fileList = new ArrayList<>();
            if (files != null) {
                for (File f : files) {
                    fileList.add(Map.of(
                            "name", f.getName(),
                            "size", f.length(),
                            "path", "data/raw/" + f.getName()
                    ));
                }
            }
            
            return ResponseEntity.ok(Map.of("files", fileList));
        } catch (Exception e) {
            logger.error("Error listing files", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/dataset/stats")
    public ResponseEntity<?> getDatasetStats() {
        try {
            File dataDir = new File("data/raw");
            if (!dataDir.exists()) {
                return ResponseEntity.ok(Map.of("stats", new Object[0]));
            }

            File[] files = dataDir.listFiles((d, name) -> name.endsWith(".log"));
            List<Map<String, Object>> stats = new ArrayList<>();

            if (files != null) {
                for (File f : files) {
                    long lines = countLines(f);
                    stats.add(Map.of(
                            "name", f.getName(),
                            "size", f.length(),
                            "sizeReadable", formatBytes(f.length()),
                            "lines", lines
                    ));
                }
            }

            return ResponseEntity.ok(Map.of("stats", stats));
        } catch (Exception e) {
            logger.error("Error getting dataset stats", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    private long countLines(File file) throws Exception {
        long count = 0;
        try (java.util.Scanner scanner = new java.util.Scanner(file)) {
            while (scanner.hasNextLine()) {
                scanner.nextLine();
                count++;
            }
        }
        return count;
    }

    private String formatBytes(long bytes) {
        if (bytes <= 0) return "0 B";
        final String[] units = new String[]{"B", "KB", "MB", "GB"};
        int digitGroups = (int) (Math.log10(bytes) / Math.log10(1024));
        return String.format("%.1f %s", bytes / Math.pow(1024, digitGroups), units[digitGroups]);
    }
}
