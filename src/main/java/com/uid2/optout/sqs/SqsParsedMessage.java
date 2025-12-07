package com.uid2.optout.sqs;

import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Represents a parsed SQS message containing opt-out data.
 */
public class SqsParsedMessage {
    private final Message originalMessage;
    private final byte[] hashBytes;
    private final byte[] idBytes;
    private final long timestamp;
    private final String email;
    private final String phone;
    private final String clientIp;
    private final String traceId;

    public SqsParsedMessage(Message originalMessage, byte[] hashBytes, byte[] idBytes, long timestamp, String email, String phone, String clientIp, String traceId) {
        this.originalMessage = originalMessage;
        this.hashBytes = hashBytes;
        this.idBytes = idBytes;
        this.timestamp = timestamp;
        this.email = email;
        this.phone = phone;
        this.clientIp = clientIp;
        this.traceId = traceId;
    }

    public Message getOriginalMessage() {
        return originalMessage;
    }

    public byte[] getHashBytes() {
        return hashBytes;
    }

    public byte[] getIdBytes() {
        return idBytes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getEmail() {
        return email;
    }

    public String getPhone() {
        return phone;
    }

    public String getClientIp() {
        return clientIp;
    }

    public String getTraceId() {
        return traceId;
    }
}

