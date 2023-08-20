package com.uid2.optout.web;

public class UnexpectedStatusCodeException extends RuntimeException {
    private final int statusCode;

    public UnexpectedStatusCodeException(int statusCode) {
        super("Unexpected status code: " + statusCode);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
