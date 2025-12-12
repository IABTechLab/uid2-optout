package com.uid2.optout.sqs;

import com.uid2.optout.delta.S3UploadService;
import com.uid2.optout.delta.StopReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

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
    private final SqsBatchProcessor batchProcessor;
    private int maxMessagesPerWindow;

    public SqsWindowReader(SqsClient sqsClient, String queueUrl, int maxMessagesPerPoll, 
                          int visibilityTimeout, int deltaWindowSeconds, int maxMessagesPerWindow,
                          S3UploadService malformedRequestUploadService, String malformedRequestsS3Path,
                          int replicaId) {
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.maxMessagesPerPoll = maxMessagesPerPoll;
        this.visibilityTimeout = visibilityTimeout;
        this.deltaWindowSeconds = deltaWindowSeconds;
        this.maxMessagesPerWindow = maxMessagesPerWindow;
        this.batchProcessor = new SqsBatchProcessor(sqsClient, queueUrl, deltaWindowSeconds, 
            malformedRequestUploadService, malformedRequestsS3Path, replicaId);
        LOGGER.info("initialized: maxMessagesPerWindow={}, maxMessagesPerPoll={}, visibilityTimeout={}, deltaWindowSeconds={}",
                maxMessagesPerWindow, maxMessagesPerPoll, visibilityTimeout, deltaWindowSeconds);
    }

    /**
     * Result of reading messages for a 5-minute window.
     */
    public static class WindowReadResult {
        private final List<SqsParsedMessage> messages;
        private final long windowStart;
        private final StopReason stopReason;
        private final int rawMessagesRead;  // total messages pulled from SQS
        
        private WindowReadResult(List<SqsParsedMessage> messages, long windowStart, StopReason stopReason, int rawMessagesRead) {
            this.messages = messages;
            this.windowStart = windowStart;
            this.stopReason = stopReason;
            this.rawMessagesRead = rawMessagesRead;
        }
        
        public static WindowReadResult withMessages(List<SqsParsedMessage> messages, long windowStart, int rawMessagesRead) {
            return new WindowReadResult(messages, windowStart, StopReason.NONE, rawMessagesRead);
        }
        
        public static WindowReadResult queueEmpty(List<SqsParsedMessage> messages, long windowStart, int rawMessagesRead) {
            return new WindowReadResult(messages, windowStart, StopReason.QUEUE_EMPTY, rawMessagesRead);
        }
        
        public static WindowReadResult messagesTooRecent(List<SqsParsedMessage> messages, long windowStart, int rawMessagesRead) {
            return new WindowReadResult(messages, windowStart, StopReason.MESSAGES_TOO_RECENT, rawMessagesRead);
        }
        
        public static WindowReadResult messageLimitExceeded(List<SqsParsedMessage> messages, long windowStart, int rawMessagesRead) {
            return new WindowReadResult(messages, windowStart, StopReason.MESSAGE_LIMIT_EXCEEDED, rawMessagesRead);
        }
        
        public List<SqsParsedMessage> getMessages() { return messages; }
        public long getWindowStart() { return windowStart; }
        public boolean isEmpty() { return messages.isEmpty(); }
        public StopReason getStopReason() { return stopReason; }
        /** Total raw messages pulled from SQS */
        public int getRawMessagesRead() { return rawMessagesRead; }
    }

    /**
     * Reads messages from SQS for one complete 5-minute window.
     * Keeps reading batches and accumulating messages until:
     * - We discover the next window
     * - Queue is empty (no more messages)
     * - Messages are too recent (all messages younger than deltaWindowSeconds)
     * - Message count exceeds maxMessagesPerWindow
     * 
     * @return WindowReadResult with messages for the window, or empty if done
     */
    public WindowReadResult readWindow() throws IOException {
        List<SqsParsedMessage> windowMessages = new ArrayList<>();
        long currentWindowStart = 0;
        int batchNumber = 0;
        int rawMessagesRead = 0;  // track total messages pulled from SQS
        
        while (true) {
            if (windowMessages.size() >= maxMessagesPerWindow) {
                LOGGER.warn("high_message_volume: message limit exceeded while reading window, {} messages >= limit {}", windowMessages.size(), maxMessagesPerWindow);
                return WindowReadResult.messageLimitExceeded(windowMessages, currentWindowStart, rawMessagesRead);
            }
            
            // Read one batch from SQS (up to 10 messages)
            List<Message> rawBatch = SqsMessageOperations.receiveMessagesFromSqs(
                this.sqsClient, this.queueUrl, this.maxMessagesPerPoll, this.visibilityTimeout);
            
            if (rawBatch.isEmpty()) {
                return WindowReadResult.queueEmpty(windowMessages, currentWindowStart, rawMessagesRead);
            }
            
            rawMessagesRead += rawBatch.size();
            
            // parse, validate, filter
            SqsBatchProcessor.BatchProcessingResult batchResult = batchProcessor.processBatch(rawBatch, batchNumber++);
            
            if (!batchResult.hasMessages()) {
                if (batchResult.getStopReason() == StopReason.MESSAGES_TOO_RECENT) {
                    return WindowReadResult.messagesTooRecent(windowMessages, currentWindowStart, rawMessagesRead);
                }
                // Corrupt messages were deleted, continue reading
                continue;
            }
            
            // Add eligible messages to current window
            boolean newWindow = false;
            for (SqsParsedMessage msg : batchResult.getMessages()) {
                long msgWindowStart = msg.timestamp();
                
                // Discover start of window
                if (currentWindowStart == 0) {
                    currentWindowStart = msgWindowStart;
                }

                // Discover next window
                if (msgWindowStart > currentWindowStart + this.deltaWindowSeconds) {
                    newWindow = true;
                }
                
                windowMessages.add(msg);
            }

            if (newWindow) {
                return WindowReadResult.withMessages(windowMessages, currentWindowStart, rawMessagesRead);
            }
        }
    }
}
