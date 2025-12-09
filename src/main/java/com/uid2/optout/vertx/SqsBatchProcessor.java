package com.uid2.optout.vertx;

import com.uid2.shared.optout.OptOutUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
     * Result of processing a batch of messages from SQS.
     * Encapsulates eligible messages and metadata about the processing.
     */
    public static class BatchProcessingResult {
        private final List<SqsParsedMessage> eligibleMessages;
        private final boolean shouldStopProcessing;
        
        private BatchProcessingResult(List<SqsParsedMessage> eligibleMessages, boolean shouldStopProcessing) {
            this.eligibleMessages = eligibleMessages;
            this.shouldStopProcessing = shouldStopProcessing;
        }
        
        public static BatchProcessingResult withEligibleMessages(List<SqsParsedMessage> messages) {
            return new BatchProcessingResult(messages, false);
        }
        
        public static BatchProcessingResult stopProcessing() {
            return new BatchProcessingResult(new ArrayList<>(), true);
        }
        
        public static BatchProcessingResult empty() {
            return new BatchProcessingResult(new ArrayList<>(), false);
        }
        
        public boolean isEmpty() {
            return eligibleMessages.isEmpty();
        }
        
        public boolean shouldStopProcessing() {
            return shouldStopProcessing;
        }
        
        public List<SqsParsedMessage> getEligibleMessages() {
            return eligibleMessages;
        }
    }

    /**
     * Processes a batch of messages: parses, validates, cleans up invalid messages, 
     * and filters for eligible messages based on age threshold (message is less than 5 minutes old)
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
                LOGGER.error("Found {} invalid messages in batch {} (failed parsing). Deleting from queue.", 
                    invalidMessages.size(), batchNumber);
                SqsMessageOperations.deleteMessagesFromSqs(this.sqsClient, this.queueUrl, invalidMessages);
            }
        }
        
        // If no valid messages, return empty result
        if (parsedBatch.isEmpty()) {
            LOGGER.warn("No valid messages in batch {} (all failed parsing)", batchNumber);
            return BatchProcessingResult.empty();
        }

        // Check if the oldest message in this batch is too recent
        long currentTime = OptOutUtils.nowEpochSeconds();
        SqsParsedMessage oldestMessage = parsedBatch.get(0);
        long messageAge = currentTime - oldestMessage.timestamp();
        
        if (messageAge < this.deltaWindowSeconds) {
            // Signal to stop processing - messages are too recent
            return BatchProcessingResult.stopProcessing();
        }

        // Filter for eligible messages (>= 5 minutes old)
        List<SqsParsedMessage> eligibleMessages = filterEligibleMessages(parsedBatch, currentTime);

        if (eligibleMessages.isEmpty()) {
            LOGGER.debug("No eligible messages in batch {} (all too recent)", batchNumber);
            return BatchProcessingResult.empty();
        }

        return BatchProcessingResult.withEligibleMessages(eligibleMessages);
    }

    /**
     * Filters messages to only include those where sufficient time has elapsed.
.    * 
     * @param messages List of parsed messages
     * @param currentTime Current time in seconds
     * @return List of messages that meet the time threshold
     */
    public List<SqsParsedMessage> filterEligibleMessages(
            List<SqsParsedMessage> messages, 
            long currentTime) {
        
        List<SqsParsedMessage> eligibleMessages = new ArrayList<>();
        
        for (SqsParsedMessage pm : messages) {
            if (currentTime - pm.timestamp() >= this.deltaWindowSeconds) {
                eligibleMessages.add(pm);
            }
        }
        
        return eligibleMessages;
    }

    /**
     * Identifies messages that failed to parse by comparing the original batch with parsed results.
     * 
     * @param originalBatch The original list of messages from SQS
     * @param parsedBatch The list of successfully parsed messages
     * @return List of messages that failed to parse
     */
    private List<Message> identifyInvalidMessages(List<Message> originalBatch, List<SqsParsedMessage> parsedBatch) {
        // Create a set of message IDs from successfully parsed messages
        Set<String> validMessageIds = new HashSet<>();
        for (SqsParsedMessage parsed : parsedBatch) {
            validMessageIds.add(parsed.originalMessage().messageId());
        }
        
        // Find messages that were not successfully parsed
        List<Message> invalidMessages = new ArrayList<>();
        for (Message msg : originalBatch) {
            if (!validMessageIds.contains(msg.messageId())) {
                invalidMessages.add(msg);
            }
        }
        
        return invalidMessages;
    }
}

