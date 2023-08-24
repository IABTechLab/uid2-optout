package com.uid2.optout.util;

public enum HttpMethod {
    GET, POST;

    public static HttpMethod fromString(String method) {
        if (method == null) {
            return null;
        }
        return HttpMethod.valueOf(method.toUpperCase());
    }
}
