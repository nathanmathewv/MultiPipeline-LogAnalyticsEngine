package com.example.multietl.pipelines.base;

public enum QueryType {
    DAILY_TRAFFIC_SUMMARY("daily_traffic_summary"),
    TOP_RESOURCES("top_resources"),
    HOURLY_ERROR_ANALYSIS("hourly_error_analysis");

    private final String key;

    QueryType(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
