package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.health.HealthComponent;
import com.uid2.shared.health.HealthManager;
import com.uid2.shared.optout.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class OptOutSqsLogProducer extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutSqsLogProducer.class);
    private final HealthComponent healthComponent = HealthManager.instance.registerComponent("sqs-log-producer");
    
    private final SqsClient sqsClient;
    private final String queueUrl;
    private final String eventDeltaProduced;
    private final int pollIntervalMs;
    private final int maxMessagesPerPoll;
    private final int visibilityTimeout;
    private final int deltaWindowSeconds; // Time window for each delta file (e.g., 5 minutes = 300 seconds)
    private final int replicaId;
    private final ICloudStorage cloudStorage;
    private final OptOutCloudSync cloudSync;
    
    private Counter counterDeltaProduced = Counter
        .builder("uid2_optout_sqs_delta_produced_total")
        .description("counter for how many optout delta files are produced from SQS")
        .register(Metrics.globalRegistry);
    
    private Counter counterEntriesProcessed = Counter
        .builder("uid2_optout_sqs_entries_processed_total")
        .description("counter for how many optout entries are processed from SQS")
        .register(Metrics.globalRegistry);
    
    private ByteBuffer buffer;
    private ByteArrayOutputStream currentDeltaStream = null;
    private String currentDeltaName = null;
    private Long currentDeltaWindowStart = null; // Unix epoch seconds for the start of current delta's time window
    private boolean writeInProgress = false;
    private boolean shutdownInProgress = false;
    
    private long pollTimerId = -1;
    
    // Track messages in current delta - only delete from SQS after successful S3 upload
    private final List<Message> currentDeltaMessages = new ArrayList<>();
    
    public OptOutSqsLogProducer(JsonObject jsonConfig, ICloudStorage cloudStorage, OptOutCloudSync cloudSync) throws IOException {
        this(jsonConfig, cloudStorage, cloudSync, Const.Event.DeltaProduced);
    }
    
    public OptOutSqsLogProducer(JsonObject jsonConfig, ICloudStorage cloudStorage, OptOutCloudSync cloudSync, String eventDeltaProduced) throws IOException {
        this.eventDeltaProduced = eventDeltaProduced;
        this.replicaId = OptOutUtils.getReplicaId(jsonConfig);
        this.cloudStorage = cloudStorage;
        this.cloudSync = cloudSync;
        
        // Initialize SQS client
        this.queueUrl = jsonConfig.getString(Const.Config.OptOutSqsQueueUrlProp);
        if (this.queueUrl == null || this.queueUrl.isEmpty()) {
            throw new IOException("SQS queue URL not configured");
        }
        
        this.sqsClient = SqsClient.builder().build();
        LOGGER.info("SQS client initialized for queue: " + this.queueUrl);
        
        // SQS polling configuration
        this.pollIntervalMs = jsonConfig.getInteger(Const.Config.OptOutSqsPollIntervalMsProp, 5000); // 5 seconds default
        this.maxMessagesPerPoll = jsonConfig.getInteger(Const.Config.OptOutSqsMaxMessagesPerPollProp, 10); // SQS max is 10
        this.visibilityTimeout = jsonConfig.getInteger(Const.Config.OptOutSqsVisibilityTimeoutProp, 300); // 5 minutes default
        
        // Delta time window configuration - each delta file represents this many seconds of opt-out data
        this.deltaWindowSeconds = jsonConfig.getInteger(Const.Config.OptOutSqsDeltaWindowSecondsProp, 300); // 5 minutes default
        
        // Initialize buffer (same value as OptOutLogProducer)
        int bufferSize = jsonConfig.getInteger(Const.Config.OptOutProducerBufferSizeProp);
        this.buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
        
        LOGGER.info("SQS Log Producer initialized - poll interval: {}ms, max messages: {}, visibility timeout: {}s, delta window: {}s, replica id: {}",
            this.pollIntervalMs, this.maxMessagesPerPoll, this.visibilityTimeout, this.deltaWindowSeconds, this.replicaId);
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        LOGGER.info("Starting SQS Log Producer...");
        
        try {
            // Start SQS polling timer - delta rotation happens automatically based on message timestamps
            this.pollTimerId = vertx.setPeriodic(this.pollIntervalMs, id -> this.pollSqsAndWrite());
            
            this.healthComponent.setHealthStatus(true);
            startPromise.complete();
            LOGGER.info("SQS Log Producer deployed successfully - polling every {}ms, delta window {}s", 
                this.pollIntervalMs, this.deltaWindowSeconds);
            
        } catch (Exception e) {
            LOGGER.error("Failed to start SQS Log Producer", e);
            this.healthComponent.setHealthStatus(false, e.getMessage());
            startPromise.fail(e);
        }
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        LOGGER.info("Stopping SQS Log Producer...");
        this.shutdownInProgress = true;
        
        // Cancel polling timer
        if (this.pollTimerId >= 0) {
            vertx.cancelTimer(this.pollTimerId);
        }
        
        // Upload current delta if any
        vertx.executeBlocking(promise -> {
            try {
                this.uploadCurrentDelta(true);
                promise.complete();
            } catch (Exception e) {
                LOGGER.error("Error during shutdown upload", e);
                promise.fail(e);
            }
        }, res -> {
            // Close SQS client
            if (this.sqsClient != null) {
                try {
                    this.sqsClient.close();
                    LOGGER.info("SQS client closed");
                } catch (Exception e) {
                    LOGGER.error("Error closing SQS client", e);
                }
            }
            
            stopPromise.complete();
            LOGGER.info("SQS Log Producer stopped");
        });
    }
    
    private void pollSqsAndWrite() {
        if (this.shutdownInProgress || this.writeInProgress) {
            return;
        }
        
        this.writeInProgress = true;
        
        vertx.executeBlocking(promise -> {
            try {
                List<Message> messages = this.receiveMessagesFromSqs();
                
                if (messages.isEmpty()) {
                    promise.complete(0);
                    return;
                }
                
                // Process messages based on their timestamps, creating new deltas as needed
                int totalProcessed = this.processMessagesWithTimeWindowing(messages);
                
                promise.complete(totalProcessed);
                
            } catch (Exception e) {
                LOGGER.error("Error in SQS poll and write", e);
                promise.fail(e);
            }
        }, res -> {
            this.writeInProgress = false;
            
            if (res.succeeded()) {
                int written = (Integer) res.result();
                if (written > 0) {
                    LOGGER.debug("Processed {} entries from SQS", written);
                    this.counterEntriesProcessed.increment(written);
                }
            } else {
                LOGGER.error("Failed to process SQS messages", res.cause());
            }
        });
    }
    
    private List<Message> receiveMessagesFromSqs() {
        try {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(this.queueUrl)
                .maxNumberOfMessages(this.maxMessagesPerPoll)
                .visibilityTimeout(this.visibilityTimeout)
                .waitTimeSeconds(0) // Non-blocking poll
                .build();
            
            ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);
            
            LOGGER.debug("Received {} messages from SQS", response.messages().size());
            return response.messages();
            
        } catch (Exception e) {
            LOGGER.error("Error receiving messages from SQS", e);
            return new ArrayList<>();
        }
    }
    
    // Process messages with time-based windowing - creates multiple deltas if messages span multiple time windows
    private int processMessagesWithTimeWindowing(List<Message> messages) {
        // Parse and sort messages by timestamp
        List<ParsedMessage> parsedMessages = new ArrayList<>();
        for (Message message : messages) {
            try {
                JsonObject body = new JsonObject(message.body());
                String identityHash = body.getString("identity_hash");
                String advertisingId = body.getString("advertising_id");
                Long timestamp = body.getLong("timestamp"); // todo where is this from?
                
                if (identityHash == null || advertisingId == null || timestamp == null) {
                    LOGGER.error("Invalid message format, skipping: {}", message.body());
                    continue;
                }
                
                byte[] hashBytes = OptOutUtils.base64StringTobyteArray(identityHash);
                byte[] idBytes = OptOutUtils.base64StringTobyteArray(advertisingId);
                
                if (hashBytes == null || idBytes == null) {
                    LOGGER.error("Invalid base64 encoding, skipping message");
                    continue;
                }
                
                parsedMessages.add(new ParsedMessage(message, hashBytes, idBytes, timestamp));
            } catch (Exception e) {
                LOGGER.error("Error parsing SQS message", e);
            }
        }
        
        // Sort by timestamp
        parsedMessages.sort((a, b) -> Long.compare(a.timestamp, b.timestamp));
        
        int totalProcessed = 0;
        
        // Process each message, rotating deltas as needed based on time windows
        for (ParsedMessage parsed : parsedMessages) {
            try {
                // Check if we need to rotate to a new delta based on the message timestamp
                if (this.shouldRotateDelta(parsed.timestamp)) {
                    // Upload current delta if it exists (this will delete messages from SQS after successful upload)
                    if (this.currentDeltaStream != null) {
                        this.uploadCurrentDelta(false);
                    }
                    // Start a new delta for this time window
                    this.startNewDelta(parsed.timestamp);
                }
                
                // Ensure we have a delta stream
                if (this.currentDeltaStream == null) {
                    this.startNewDelta(parsed.timestamp);
                }
                
                // Write entry to current delta
                this.writeEntryToDelta(parsed.hashBytes, parsed.idBytes, parsed.timestamp);
                
                // Track this message with the current delta - will be deleted from SQS after successful S3 upload
                this.currentDeltaMessages.add(parsed.originalMessage);
                totalProcessed++;
                
            } catch (Exception e) {
                LOGGER.error("Error processing message with timestamp " + parsed.timestamp, e);
            }
        }
        
        return totalProcessed;
    }
    
    // Helper class to hold parsed message data
    private static class ParsedMessage {
        final Message originalMessage;
        final byte[] hashBytes;
        final byte[] idBytes;
        final long timestamp;
        
        ParsedMessage(Message originalMessage, byte[] hashBytes, byte[] idBytes, long timestamp) {
            this.originalMessage = originalMessage;
            this.hashBytes = hashBytes;
            this.idBytes = idBytes;
            this.timestamp = timestamp;
        }
    }
    
    // Check if we should rotate to a new delta based on message timestamp
    private boolean shouldRotateDelta(long messageTimestamp) {
        if (this.currentDeltaWindowStart == null) {
            return false; // No current delta
        }
        
        long windowEnd = this.currentDeltaWindowStart + this.deltaWindowSeconds;
        return messageTimestamp >= windowEnd;
    }
    
    // Start a new delta for the time window containing the given timestamp
    private void startNewDelta(long messageTimestamp) throws IOException {
        // Calculate the window start for this message (round down to delta window boundary)
        long windowStart = (messageTimestamp / this.deltaWindowSeconds) * this.deltaWindowSeconds; // todo - should we keep fixed 5 minute or only produce deltas when there are messages
        
        this.currentDeltaWindowStart = windowStart;
        this.currentDeltaName = OptOutUtils.newDeltaFileName(this.replicaId);
        this.currentDeltaStream = new ByteArrayOutputStream();
        
        // Add a special first entry with null hash and the window start timestamp
        buffer.put(OptOutUtils.nullHashBytes);
        buffer.put(OptOutUtils.nullHashBytes);
        buffer.putLong(windowStart);
        buffer.flip();
        byte[] firstEntry = new byte[buffer.remaining()];
        buffer.get(firstEntry);
        this.currentDeltaStream.write(firstEntry);
        buffer.clear();
        
        LOGGER.info("Started new delta: {} for time window [{}, {})", 
            this.currentDeltaName, windowStart, windowStart + this.deltaWindowSeconds);
    }
    
    // Write a single entry to the current delta
    private void writeEntryToDelta(byte[] hashBytes, byte[] idBytes, long timestamp) throws IOException {
        this.checkBufferSize(OptOutConst.EntrySize);
        
        OptOutEntry.writeTo(buffer, hashBytes, idBytes, timestamp);
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        this.currentDeltaStream.write(data);
        buffer.clear();
    }
    
    private void deleteMessagesFromSqs(List<Message> messages) {
        if (messages.isEmpty()) {
            return;
        }
        
        vertx.executeBlocking(promise -> {
            try {
                // SQS allows batch delete of up to 10 messages at a time
                List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
                int batchId = 0;
                int totalDeleted = 0;
                
                for (Message msg : messages) {
                    entries.add(DeleteMessageBatchRequestEntry.builder()
                        .id(String.valueOf(batchId++))
                        .receiptHandle(msg.receiptHandle())
                        .build());
                    
                    // Send batch when we reach 10 messages or at the end
                    if (entries.size() == 10 || batchId == messages.size()) {
                        DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder()
                            .queueUrl(this.queueUrl)
                            .entries(entries)
                            .build();
                        
                        DeleteMessageBatchResponse deleteResponse = sqsClient.deleteMessageBatch(deleteRequest);
                        
                        if (!deleteResponse.failed().isEmpty()) {
                            LOGGER.error("Failed to delete {} messages from SQS", deleteResponse.failed().size());
                        } else {
                            totalDeleted += entries.size();
                        }
                        
                        entries.clear();
                    }
                }
                
                LOGGER.debug("Deleted {} messages from SQS", totalDeleted);
                promise.complete();
                
            } catch (Exception e) {
                LOGGER.error("Error deleting messages from SQS", e);
                promise.fail(e);
            }
        });
    }
    
    // Upload current delta to S3 (called when time window is complete or during shutdown)
    private void uploadCurrentDelta(boolean shuttingDown) {
        if (this.currentDeltaStream == null || this.currentDeltaName == null) {
            if (shuttingDown) {
                this.shutdownInProgress = true;
            }
            return;
        }
        
        try {
            // Add a special last entry with ffff hash and the window end timestamp
            long endTimestamp = this.currentDeltaWindowStart != null ? 
                this.currentDeltaWindowStart + this.deltaWindowSeconds : 
                OptOutUtils.nowEpochSeconds();
            
            buffer.put(OptOutUtils.onesHashBytes);
            buffer.put(OptOutUtils.onesHashBytes);
            buffer.putLong(endTimestamp);
            buffer.flip();
            byte[] lastEntry = new byte[buffer.remaining()];
            buffer.get(lastEntry);
            this.currentDeltaStream.write(lastEntry);
            buffer.clear();
            
            // Get the complete delta data
            byte[] deltaData = this.currentDeltaStream.toByteArray();
            
            // Generate S3 path using cloud sync
            String s3Path = this.cloudSync.toCloudPath(this.currentDeltaName);
            
            // Upload to S3 using ICloudStorage
            LOGGER.info("Uploading delta to S3: {} ({} bytes, window: [{}, {}))", 
                s3Path, deltaData.length, this.currentDeltaWindowStart, endTimestamp);
            
            boolean uploadSucceeded = false;
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(deltaData)) {
                this.cloudStorage.upload(inputStream, s3Path);
                LOGGER.info("Successfully uploaded delta to S3: {}", s3Path);
                uploadSucceeded = true;
                
                // Publish event
                this.publishDeltaProducedEvent(this.currentDeltaName);
                
            } catch (Exception uploadEx) {
                LOGGER.error("Failed to upload delta to S3: " + uploadEx.getMessage(), uploadEx);
                // Don't delete messages from SQS if upload failed - they'll be retried
            }
            
            // CRITICAL: Only delete messages from SQS after successful S3 upload
            if (uploadSucceeded && !this.currentDeltaMessages.isEmpty()) {
                LOGGER.info("Deleting {} messages from SQS after successful S3 upload", this.currentDeltaMessages.size());
                this.deleteMessagesFromSqs(new ArrayList<>(this.currentDeltaMessages));
                this.currentDeltaMessages.clear();
            }
            
            // Close the stream and reset
            this.currentDeltaStream.close();
            this.currentDeltaStream = null;
            this.currentDeltaName = null;
            this.currentDeltaWindowStart = null;
            
        } catch (Exception ex) {
            LOGGER.error("Error uploading delta: " + ex.getMessage(), ex);
            // Don't clear currentDeltaMessages - messages will remain in SQS for retry
        }
        
        if (shuttingDown) {
            this.shutdownInProgress = true;
        }
    }
    
    private void publishDeltaProducedEvent(String newDelta) {
        this.counterDeltaProduced.increment();
        vertx.eventBus().publish(this.eventDeltaProduced, newDelta);
        LOGGER.info("Published delta.produced event for: {}", newDelta);
    }
    
    private void checkBufferSize(int dataSize) {
        ByteBuffer b = this.buffer;
        if (b.capacity() < dataSize) {
            int newCapacity = Integer.highestOneBit(dataSize) << 1;
            LOGGER.warn("Expanding buffer size: current {}, need {}, new {}", b.capacity(), dataSize, newCapacity);
            this.buffer = ByteBuffer.allocate(newCapacity).order(ByteOrder.LITTLE_ENDIAN);
        }
    }
}

