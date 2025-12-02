package com.uid2.optout.vertx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads messages from SQS for complete 5-minute time windows.
 * Handles accumulation of all messages for a window before returning.
 */
public class SqsWindowReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsWindowReader.class);
    
    private final SqsClient sqsClient;
    private final String queueUrl;
    private final int maxMessagesPerPoll;
    private final int visibilityTimeout;
    private final int deltaWindowSeconds;
    private final SqsBatchProcessor batchProcessor;
    private int maxMessagesPerWindow;

    public SqsWindowReader(SqsClient sqsClient, String queueUrl, int maxMessagesPerPoll, 
                          int visibilityTimeout, int deltaWindowSeconds, int maxMessagesPerWindow) {
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.maxMessagesPerPoll = maxMessagesPerPoll;
        this.visibilityTimeout = visibilityTimeout;
        this.deltaWindowSeconds = deltaWindowSeconds;
        this.maxMessagesPerWindow = maxMessagesPerWindow;
        this.batchProcessor = new SqsBatchProcessor(sqsClient, queueUrl, deltaWindowSeconds);
    }

    /**
     * Update the max messages limit (e.g., after config reload).
     */
    public void setMaxMessagesPerWindow(int maxMessagesPerWindow) {
        this.maxMessagesPerWindow = maxMessagesPerWindow;
        LOGGER.info("Updated maxMessagesPerWindow to {}", maxMessagesPerWindow);
    }

    /**
     * Result of reading messages for a 5-minute window.
     */
    public static class WindowReadResult {
        private final List<SqsParsedMessage> messages;
        private final long windowStart;
        private final boolean stoppedDueToRecentMessages;
        private final boolean exceededMessageLimit;
        
        public WindowReadResult(List<SqsParsedMessage> messages, long windowStart, 
                               boolean stoppedDueToRecentMessages, boolean exceededMessageLimit) {
            this.messages = messages;
            this.windowStart = windowStart;
            this.stoppedDueToRecentMessages = stoppedDueToRecentMessages;
            this.exceededMessageLimit = exceededMessageLimit;
        }
        
        public List<SqsParsedMessage> getMessages() { return messages; }
        public long getWindowStart() { return windowStart; }
        public boolean isEmpty() { return messages.isEmpty(); }
        public boolean stoppedDueToRecentMessages() { return stoppedDueToRecentMessages; }
        public boolean exceededMessageLimit() { return exceededMessageLimit; }
    }

    /**
     * Reads messages from SQS for one complete 5-minute window.
     * Keeps reading batches and accumulating messages until:
     * - We discover the next window
     * - Queue is empty (no more messages)
     * - Messages are too recent (all messages younger than 5 minutes)
     * - Message count exceeds maxMessagesPerWindow
     * 
     * @return WindowReadResult with messages for the window, or empty if done
     */
    public WindowReadResult readWindow() {
        List<SqsParsedMessage> windowMessages = new ArrayList<>();
        long currentWindowStart = 0;
        
        while (true) {

            if (windowMessages.size() >= maxMessagesPerWindow) {
                LOGGER.warn("Message limit exceeded: {} messages >= limit {}. Stopping to prevent memory exhaustion.",
                    windowMessages.size(), maxMessagesPerWindow);
                return new WindowReadResult(windowMessages, currentWindowStart, false, true);
            }
            
            // Read one batch from SQS (up to 10 messages)
            List<Message> rawBatch = SqsMessageOperations.receiveMessagesFromSqs(
                this.sqsClient, this.queueUrl, this.maxMessagesPerPoll, this.visibilityTimeout);
            
            if (rawBatch.isEmpty()) {
                // Queue empty - return what we have
                return new WindowReadResult(windowMessages, currentWindowStart, false, false);
            }
            
            // Process batch: parse, validate, filter
            SqsBatchProcessor.BatchProcessingResult batchResult = batchProcessor.processBatch(rawBatch, 0);
            
            if (batchResult.isEmpty()) {
                if (batchResult.shouldStopProcessing()) {
                    // Messages too recent - return what we have
                    return new WindowReadResult(windowMessages, currentWindowStart, true, false);
                }
                // corrupt messages deleted, read next messages
                continue;
            }
            
            // Add eligible messages to current window
            boolean newWindow = false;
            for (SqsParsedMessage msg : batchResult.getEligibleMessages()) {
                long msgWindowStart = (msg.getTimestamp() / this.deltaWindowSeconds) * this.deltaWindowSeconds;
                
                // discover start of window
                if (currentWindowStart == 0) {
                    currentWindowStart = msgWindowStart;
                }

                // discover new window
                if (msgWindowStart > currentWindowStart + this.deltaWindowSeconds) {
                    newWindow = true;
                }
                
                windowMessages.add(msg);
                
                // Check limit after each message addition
                if (windowMessages.size() >= maxMessagesPerWindow) {
                    LOGGER.warn("Message limit exceeded during batch: {} messages >= limit {}",
                        windowMessages.size(), maxMessagesPerWindow);
                    return new WindowReadResult(windowMessages, currentWindowStart, false, true);
                }
            }

            if (newWindow) {
                // close current window and return
                return new WindowReadResult(windowMessages, currentWindowStart, false, false);
            }
        }
    }
}
