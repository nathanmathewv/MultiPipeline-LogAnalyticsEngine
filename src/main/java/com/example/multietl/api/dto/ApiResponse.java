package com.example.multietl.api.dto;

import java.util.Map;

public class ApiResponse {
    public String status;
    public String message;
    public Object data;
    public Map<String, String> errors;

    public ApiResponse(String status, String message, Object data) {
        this.status = status;
        this.message = message;
        this.data = data;
    }

    public ApiResponse(String status, String message, Object data, Map<String, String> errors) {
        this.status = status;
        this.message = message;
        this.data = data;
        this.errors = errors;
    }

    public static ApiResponse success(Object data) {
        return new ApiResponse("success", "Operation completed successfully", data);
    }

    public static ApiResponse error(String message) {
        return new ApiResponse("error", message, null);
    }

    public static ApiResponse error(String message, Map<String, String> errors) {
        return new ApiResponse("error", message, null, errors);
    }
}
