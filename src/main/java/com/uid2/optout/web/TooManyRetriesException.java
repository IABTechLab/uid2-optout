package com.uid2.optout.web;

public class TooManyRetriesException extends Exception {
    public TooManyRetriesException(int retries) {
        super("Too many retries: " + retries);
    }
}
