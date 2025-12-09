package com.uid2.optout.vertx;

import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Represents a parsed SQS message containing opt-out data.
 */
public record SqsParsedMessage(
    Message originalMessage,
    byte[] hashBytes,
    byte[] idBytes,
    long timestamp,
    String email,
    String phone,
    String clientIp,
    String traceId
) {}
