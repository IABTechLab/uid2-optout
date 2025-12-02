package com.uid2.optout.vertx;

import com.uid2.shared.optout.OptOutUtils;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for parsing SQS messages containing opt-out data.
 */
public class SqsMessageParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsMessageParser.class);

    /**
     * Parses and sorts a list of SQS messages by timestamp.
     * 
     * @param messages List of raw SQS messages
     * @return List of parsed messages sorted by timestamp (oldest first)
     */
    public static List<SqsParsedMessage> parseAndSortMessages(List<Message> messages) {
        List<SqsParsedMessage> parsedMessages = new ArrayList<>();

        for (Message message : messages) {
            try {
                // Extract SQS system timestamp (in milliseconds), or use current time as fallback
                long timestampSeconds = extractTimestamp(message);

                // Parse message body
                JsonObject body = new JsonObject(message.body());
                String identityHash = body.getString("identity_hash");
                String advertisingId = body.getString("advertising_id");

                if (identityHash == null || advertisingId == null) {
                    LOGGER.error("Invalid message format, skipping: {}", message.body());
                    continue;
                }

                byte[] hashBytes = OptOutUtils.base64StringTobyteArray(identityHash);
                byte[] idBytes = OptOutUtils.base64StringTobyteArray(advertisingId);

                if (hashBytes == null || idBytes == null) {
                    LOGGER.error("Invalid base64 encoding, skipping message");
                    continue;
                }

                parsedMessages.add(new SqsParsedMessage(message, hashBytes, idBytes, timestampSeconds));
            } catch (Exception e) {
                LOGGER.error("Error parsing SQS message", e);
            }
        }

        // Sort by timestamp
        parsedMessages.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));

        return parsedMessages;
    }

    /**
     * Extracts timestamp from SQS message attributes, falling back to current time if unavailable.
     * 
     * @param message The SQS message
     * @return Timestamp in seconds
     */
    private static long extractTimestamp(Message message) {
        String sentTimestampStr = message.attributes().get(MessageSystemAttributeName.SENT_TIMESTAMP);
        if (sentTimestampStr == null) {
            LOGGER.warn("Message missing SentTimestamp attribute, using current time");
            return OptOutUtils.nowEpochSeconds();
        }
        return Long.parseLong(sentTimestampStr) / 1000; // ms to seconds
    }
}

