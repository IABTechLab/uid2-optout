package com.uid2.optout.sqs;

import com.uid2.optout.delta.StopReason;
import com.uid2.shared.optout.OptOutUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Applies parsing, validation, filtering, and deletion of corrupted SQS messages.
 * Used by SqsWindowReader
 */
public class SqsBatchProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsBatchProcessor.class);
    
    private final SqsClient sqsClient;
    private final String queueUrl;
    private final int deltaWindowSeconds;

    public SqsBatchProcessor(SqsClient sqsClient, String queueUrl, int deltaWindowSeconds) {
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.deltaWindowSeconds = deltaWindowSeconds;
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
    public BatchProcessingResult processBatch(List<Message> messageBatch, int batchNumber) {
        // Parse and sort messages by timestamp
        List<SqsParsedMessage> parsedBatch = SqsMessageParser.parseAndSortMessages(messageBatch);
        
        // Identify and delete corrupt messages
        if (parsedBatch.size() < messageBatch.size()) {
            List<Message> invalidMessages = identifyInvalidMessages(messageBatch, parsedBatch);
            if (!invalidMessages.isEmpty()) {
                LOGGER.error("sqs_error: found {} invalid messages in batch {}, deleting", invalidMessages.size(), batchNumber);
                SqsMessageOperations.deleteMessagesFromSqs(this.sqsClient, this.queueUrl, invalidMessages); // TODO: send to a folder in the dropped requests bucket before deleting.
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
        return currentTime - message.getTimestamp() >= this.deltaWindowSeconds;
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
                .map(p -> p.getOriginalMessage().messageId())
                .collect(Collectors.toSet());
        
        return originalBatch.stream()
                .filter(msg -> !validIds.contains(msg.messageId()))
                .collect(Collectors.toList());
    }
}
