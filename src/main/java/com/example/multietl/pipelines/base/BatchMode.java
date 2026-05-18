package com.example.multietl.pipelines.base;

public enum BatchMode {
    DAYS("days"),
    RECORDS("records");

    private final String key;

    BatchMode(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public static BatchMode from(String value) {
        if (value == null || value.isBlank()) {
            return DAYS;
        }
        String normalized = value.trim().toLowerCase();
        switch (normalized) {
            case "day":
            case "days":
            case "date":
            case "dates":
                return DAYS;
            case "record":
            case "records":
            case "row":
            case "rows":
                return RECORDS;
            default:
                throw new IllegalArgumentException("Unsupported batch mode: " + value + " (expected days or records)");
        }
    }
}
