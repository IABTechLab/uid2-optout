package com.uid2.optout.util;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqsUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsUtils.class);
    
    /**
     * Extract timestamp from SQS message (from SentTimestamp attribute)
     */
    public static long extractTimestampFromMessage(Message msg) {
        // Get SentTimestamp attribute (milliseconds)
        String sentTimestamp = msg.attributes().get(MessageSystemAttributeName.SENT_TIMESTAMP);
        if (sentTimestamp != null) {
            try {
                return Long.parseLong(sentTimestamp) / 1000;  // Convert ms to seconds
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid SentTimestamp: {}", sentTimestamp);
            }
        }
        
        // Fallback: use current time
        return System.currentTimeMillis() / 1000;
    }
}
