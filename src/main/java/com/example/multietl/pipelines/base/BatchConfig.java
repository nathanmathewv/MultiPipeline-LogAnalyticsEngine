package com.example.multietl.pipelines.base;

import java.util.Locale;

public class BatchConfig {
    private final BatchMode mode;
    private final int size;

    public BatchConfig(BatchMode mode, int size) {
        if (mode == null) {
            throw new IllegalArgumentException("batch mode is required");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("batch size must be > 0");
        }
        this.mode = mode;
        this.size = size;
    }

    public static BatchConfig days(int days) {
        return new BatchConfig(BatchMode.DAYS, days);
    }

    public static BatchConfig records(int records) {
        return new BatchConfig(BatchMode.RECORDS, records);
    }

    public static BatchConfig of(String mode, int size) {
        return new BatchConfig(BatchMode.from(mode), size);
    }

    public BatchMode getMode() {
        return mode;
    }

    public int getSize() {
        return size;
    }

    public boolean isDays() {
        return mode == BatchMode.DAYS;
    }

    public boolean isRecords() {
        return mode == BatchMode.RECORDS;
    }

    public Integer getBatchSizeDays() {
        return isDays() ? size : null;
    }

    public Integer getBatchSizeRecords() {
        return isRecords() ? size : null;
    }

    public String getModeKey() {
        return mode.getKey();
    }

    public String describe() {
        String unit = isDays() ? "day" : "record";
        return String.format(Locale.ROOT, "%s=%d %s%s", getModeKey(), size, unit, size == 1 ? "" : "s");
    }
}
