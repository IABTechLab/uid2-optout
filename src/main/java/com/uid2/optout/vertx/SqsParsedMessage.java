package com.uid2.optout.vertx;

import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Represents a parsed SQS message containing opt-out data.
 */
public class SqsParsedMessage {
    private final Message originalMessage;
    private final byte[] hashBytes;
    private final byte[] idBytes;
    private final long timestamp;

    public SqsParsedMessage(Message originalMessage, byte[] hashBytes, byte[] idBytes, long timestamp) {
        this.originalMessage = originalMessage;
        this.hashBytes = hashBytes;
        this.idBytes = idBytes;
        this.timestamp = timestamp;
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
}

