package com.uid2.optout.sqs;

import com.uid2.optout.delta.S3UploadService;
import com.uid2.optout.delta.StopReason;
import com.uid2.shared.optout.OptOutUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Applies parsing, validation, filtering, and deletion of corrupted SQS messages.
 * Used by SqsWindowReader
 */
public class SqsBatchProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsBatchProcessor.class);
    private static final String MALFORMED_FILE_PREFIX = "optout-malformed-";
    private final int deltaWindowSeconds;
    private final S3UploadService s3UploadService;
    private final String malformedRequestsS3Path;
    private final int replicaId;

    public SqsBatchProcessor(SqsClient sqsClient, String queueUrl, int deltaWindowSeconds, 
                             S3UploadService s3UploadService, String malformedRequestsS3Path,
                             int replicaId) {
        this.deltaWindowSeconds = deltaWindowSeconds;
        this.s3UploadService = s3UploadService;
        this.malformedRequestsS3Path = malformedRequestsS3Path;
        this.replicaId = replicaId;
    }

    /**
     * Result of processing a batch of (10) messages from SQS.
     * Encapsulates eligible messages and the reason for stopping (if any).
     */
    public static class BatchProcessingResult {
        private final List<SqsParsedMessage> eligibleMessages;
        private final StopReason stopReason;
        
        private BatchProcessingResult(List<SqsParsedMessage> eligibleMessages, StopReason stopReason) {
            this.eligibleMessages = eligibleMessages;
            this.stopReason = stopReason;
        }
        
        public static BatchProcessingResult withMessages(List<SqsParsedMessage> messages) {
            return new BatchProcessingResult(messages, StopReason.NONE);
        }
        
        public static BatchProcessingResult messagesTooRecent() {
            return new BatchProcessingResult(List.of(), StopReason.MESSAGES_TOO_RECENT);
        }
        
        public static BatchProcessingResult corruptMessagesDeleted() {
            return new BatchProcessingResult(List.of(), StopReason.NONE);
        }
        
        public boolean hasMessages() {
            return !eligibleMessages.isEmpty();
        }
        
        public StopReason getStopReason() {
            return stopReason;
        }
        
        public List<SqsParsedMessage> getMessages() {
            return eligibleMessages;
        }
    }

    /**
     * Processes a batch of messages: parses, validates, cleans up invalid messages, 
     * and filters for eligible messages based on age threshold (message is older than deltaWindowSeconds)
     * 
     * @param messageBatch Raw messages from SQS
     * @param batchNumber The batch number (for logging)
     * @return BatchProcessingResult containing eligible messages and processing metadata
     */
    public BatchProcessingResult processBatch(List<Message> messageBatch, int batchNumber) throws IOException {
        // Parse and sort messages by timestamp
        List<SqsParsedMessage> parsedBatch = SqsMessageParser.parseAndSortMessages(messageBatch);
        
        // Identify and delete corrupt messages
        if (parsedBatch.size() < messageBatch.size()) {
            List<Message> invalidMessages = identifyInvalidMessages(messageBatch, parsedBatch);
            if (!invalidMessages.isEmpty()) {
                LOGGER.error("sqs_error: found {} invalid messages in batch {}, uploading to S3 and deleting", invalidMessages.size(), batchNumber);
                uploadMalformedMessages(invalidMessages);
            }
        }
        
        // No valid messages after deleting corrupt ones, continue reading
        if (parsedBatch.isEmpty()) {
            LOGGER.info("no valid messages in batch {} after removing invalid messages", batchNumber);
            return BatchProcessingResult.corruptMessagesDeleted();
        }

        // Check if the oldest message in this batch is too recent
        long currentTime = OptOutUtils.nowEpochSeconds();
        SqsParsedMessage oldestMessage = parsedBatch.get(0);
        
        if (!isMessageEligible(oldestMessage, currentTime)) {
            return BatchProcessingResult.messagesTooRecent();
        }

        // Filter for eligible messages (>= deltaWindowSeconds old)
        List<SqsParsedMessage> eligibleMessages = filterEligibleMessages(parsedBatch, currentTime);

        return BatchProcessingResult.withMessages(eligibleMessages);
    }

    /**
     * Checks if a message is old enough to be processed.
     * 
     * @param message The parsed message to check
     * @param currentTime Current time in epoch seconds
     * @return true if the message is at least deltaWindowSeconds old
     */
    private boolean isMessageEligible(SqsParsedMessage message, long currentTime) {
        return currentTime - message.timestamp() >= this.deltaWindowSeconds;
    }

    /**
     * Filters messages to only include those where sufficient time has elapsed.
     * 
     * @param messages List of parsed messages
     * @param currentTime Current time in seconds
     * @return List of messages that meet the time threshold
     */
    List<SqsParsedMessage> filterEligibleMessages(List<SqsParsedMessage> messages, long currentTime) {
        return messages.stream()
                .filter(msg -> isMessageEligible(msg, currentTime))
                .collect(Collectors.toList());
    }

    /**
     * Identifies messages that failed to parse by comparing the original batch with parsed results.
     * 
     * @param originalBatch The original list of messages from SQS
     * @param parsedBatch The list of successfully parsed messages
     * @return List of messages that failed to parse
     */
    private List<Message> identifyInvalidMessages(List<Message> originalBatch, List<SqsParsedMessage> parsedBatch) {
        Set<String> validIds = parsedBatch.stream()
                .map(p -> p.originalMessage().messageId())
                .collect(Collectors.toSet());
        
        return originalBatch.stream()
                .filter(msg -> !validIds.contains(msg.messageId()))
                .collect(Collectors.toList());
    }
    
    /**
     * Uploads malformed messages to S3 and then deletes them from SQS.
     * The destination is "malformed" folder in the dropped requests S3 bucket.
     * 
     * @param invalidMessages The malformed messages to upload and delete
     */
    private void uploadMalformedMessages(List<Message> invalidMessages) throws IOException {
        if (s3UploadService == null) {
            LOGGER.error("s3_error: s3UploadService is null, skipping upload of {} malformed messages", invalidMessages.size());
            return;
        }
        
        // serialize messages to json string
        JsonArray messagesJson = new JsonArray();
        for (Message msg : invalidMessages) {
            messagesJson.add(new JsonObject()
                .put("messageId", msg.messageId())
                .put("body", msg.body())
                .put("attributes", msg.attributesAsStrings()));
        }
        
        // format file name and data
        byte[] data = messagesJson.encodePrettily().getBytes(StandardCharsets.UTF_8);
        String filename = generateMalformedMessageFileName();
        String s3Path = malformedRequestsS3Path + filename;
        
        // upload and delete messages
        try {
            s3UploadService.uploadAndDeleteMessages(data, s3Path, invalidMessages, null); 
            LOGGER.info("uploaded {} malformed messages to {}", invalidMessages.size(), s3Path);
        } catch (IOException e) {
            LOGGER.error("failed to upload and delete malformed sqs messages, path={}, filename={}, error={}", malformedRequestsS3Path, filename, e.getMessage(), e);
            throw e;
        }
    }

    private String generateMalformedMessageFileName() {
        return String.format("%s%03d_%s_%08x.json",
                MALFORMED_FILE_PREFIX,
                replicaId,
                Instant.now().truncatedTo(ChronoUnit.SECONDS).toString().replace(':', '.'),
                OptOutUtils.rand.nextInt());
    }
}
