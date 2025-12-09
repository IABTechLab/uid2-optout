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
 * Limits messages per window to prevent memory issues.
 */
public class SqsWindowReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsWindowReader.class);
    
    private final SqsClient sqsClient;
    private final String queueUrl;
    private final int maxMessagesPerPoll;
    private final int visibilityTimeout;
    private final int deltaWindowSeconds;
    private final int maxMessagesPerFile;
    private final SqsBatchProcessor batchProcessor;
    
    public SqsWindowReader(SqsClient sqsClient, String queueUrl, int maxMessagesPerPoll, 
                          int visibilityTimeout, int deltaWindowSeconds, int maxMessagesPerFile) {
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.maxMessagesPerPoll = maxMessagesPerPoll;
        this.visibilityTimeout = visibilityTimeout;
        this.deltaWindowSeconds = deltaWindowSeconds;
        this.maxMessagesPerFile = maxMessagesPerFile;
        this.batchProcessor = new SqsBatchProcessor(sqsClient, queueUrl, deltaWindowSeconds);
        LOGGER.info("SqsWindowReader initialized with: maxMessagesPerFile: {}, maxMessagesPerPoll: {}, visibilityTimeout: {}, deltaWindowSeconds: {}",
                        maxMessagesPerFile, maxMessagesPerPoll, visibilityTimeout, deltaWindowSeconds);
    }

    /**
     * Result of reading messages for a 5-minute window.
     */
    public static class WindowReadResult {
        private final List<SqsParsedMessage> messages;
        private final long windowStart;
        private final boolean stoppedDueToMessagesTooRecent;
        
        public WindowReadResult(List<SqsParsedMessage> messages, long windowStart, 
                                boolean stoppedDueToMessagesTooRecent) {
            this.messages = messages;
            this.windowStart = windowStart;
            this.stoppedDueToMessagesTooRecent = stoppedDueToMessagesTooRecent;
        }
        
        public List<SqsParsedMessage> getMessages() { return messages; }
        public long getWindowStart() { return windowStart; }
        public boolean isEmpty() { return messages.isEmpty(); }
        public boolean stoppedDueToMessagesTooRecent() { return stoppedDueToMessagesTooRecent; }
    }

    /**
     * Reads messages from SQS for one complete 5-minute window.
     * Keeps reading batches and accumulating messages until:
     * - We discover the next window
     * - Queue is empty (no more messages)
     * - Messages are too recent (all messages younger than 5 minutes)
     * - Message limit is reached (memory protection)
     * 
     * @return WindowReadResult with messages for the window, or empty if done
     */
    public WindowReadResult readWindow() {
        List<SqsParsedMessage> windowMessages = new ArrayList<>();
        long currentWindowStart = 0;
        
        while (true) {
            // Check if we've hit the message limit
            if (windowMessages.size() >= this.maxMessagesPerFile) {
                LOGGER.warn("Window message limit reached ({} messages). Truncating window starting at {} for memory protection.",
                    this.maxMessagesPerFile, currentWindowStart);
                return new WindowReadResult(windowMessages, currentWindowStart, false);
            }
            
            // Read one batch from SQS (up to 10 messages)
            List<Message> rawBatch = SqsMessageOperations.receiveMessagesFromSqs(
                this.sqsClient, this.queueUrl, this.maxMessagesPerPoll, this.visibilityTimeout);
            
            if (rawBatch.isEmpty()) {
                // Queue empty - return what we have
                return new WindowReadResult(windowMessages, currentWindowStart, false);
            }
            
            // Process batch: parse, validate, filter
            SqsBatchProcessor.BatchProcessingResult batchResult = batchProcessor.processBatch(rawBatch, 0);
            
            if (batchResult.isEmpty()) {
                if (batchResult.shouldStopProcessing()) {
                    // Messages too recent - return what we have
                    return new WindowReadResult(windowMessages, currentWindowStart, true);
                }
                // corrupt messages deleted, read next messages
                continue;
            }
            
            // Add eligible messages to current window
            boolean newWindow = false;
            for (SqsParsedMessage msg : batchResult.getEligibleMessages()) {
                long msgWindowStart = (msg.timestamp() / this.deltaWindowSeconds) * this.deltaWindowSeconds;
                
                // discover start of window
                if (currentWindowStart == 0) {
                    currentWindowStart = msgWindowStart;
                }

                // discover new window
                if (msgWindowStart > currentWindowStart + this.deltaWindowSeconds) {
                    newWindow = true;
                }
                
                windowMessages.add(msg);
            }

            if (newWindow) {
                // close current window and return
                return new WindowReadResult(windowMessages, currentWindowStart, false);
            }
        }
    }
}

