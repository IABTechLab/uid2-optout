package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.auth.InternalAuthMiddleware;
import com.uid2.shared.Utils;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.health.HealthComponent;
import com.uid2.shared.health.HealthManager;
import com.uid2.shared.optout.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.vertx.core.*;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
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
    private final int replicaId;
    private final ICloudStorage cloudStorage;
    private final OptOutCloudSync cloudSync;
    private final int maxMessagesPerPoll;
    private final int visibilityTimeout;
    private final int deltaWindowSeconds; // Time window for each delta file (5 minutes = 300 seconds)
    private final int listenPort;
    private final String internalApiKey;
    private final InternalAuthMiddleware internalAuth;

    private Counter counterDeltaProduced = Counter
        .builder("uid2_optout_sqs_delta_produced_total")
        .description("counter for how many optout delta files are produced from SQS")
        .register(Metrics.globalRegistry);

    private Counter counterEntriesProcessed = Counter
        .builder("uid2_optout_sqs_entries_processed_total")
        .description("counter for how many optout entries are processed from SQS")
        .register(Metrics.globalRegistry);

    private ByteBuffer buffer;
    private boolean shutdownInProgress = false;

    public OptOutSqsLogProducer(JsonObject jsonConfig, ICloudStorage cloudStorage, OptOutCloudSync cloudSync) throws IOException {
        this(jsonConfig, cloudStorage, cloudSync, Const.Event.DeltaProduce);
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

        // SQS Configuration
        this.maxMessagesPerPoll = 10; // SQS max is 10
        this.visibilityTimeout = jsonConfig.getInteger(Const.Config.OptOutSqsVisibilityTimeoutProp, 240); // 4 minutes default
        this.deltaWindowSeconds = 300; // Fixed 5 minutes for all deltas

        // HTTP server configuration - use port offset + 1 to avoid conflicts
        this.listenPort = Const.Port.ServicePortForOptOut + Utils.getPortOffset() + 1;
        
        // Authentication
        this.internalApiKey = jsonConfig.getString(Const.Config.OptOutInternalApiTokenProp);
        this.internalAuth = new InternalAuthMiddleware(this.internalApiKey, "optout-sqs");

        int bufferSize = jsonConfig.getInteger(Const.Config.OptOutProducerBufferSizeProp);
        this.buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public void start(Promise<Void> startPromise) {
        LOGGER.info("Starting SQS Log Producer with HTTP endpoint...");

        try {
            vertx.createHttpServer()
                    .requestHandler(createRouter())
                    .listen(listenPort, result -> {
                        if (result.succeeded()) {
                            this.healthComponent.setHealthStatus(true);
                            LOGGER.info("SQS Log Producer HTTP server started on port: {} (delta window: {}s)", 
                                listenPort, this.deltaWindowSeconds);
                            startPromise.complete();
                        } else {
                            LOGGER.error("Failed to start SQS Log Producer HTTP server", result.cause());
                            this.healthComponent.setHealthStatus(false, result.cause().getMessage());
                            startPromise.fail(result.cause());
                        }
                    });

        } catch (Exception e) {
            LOGGER.error("Failed to start SQS Log Producer", e);
            this.healthComponent.setHealthStatus(false, e.getMessage());
            startPromise.fail(e);
        }
    }

    private Router createRouter() {
        Router router = Router.router(vertx);
        
        router.post(Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .handler(internalAuth.handleWithAudit(this::handleDeltaProduce));
        
        LOGGER.info("Registered endpoint: POST {}", Endpoints.OPTOUT_DELTA_PRODUCE);
        
        return router;
    }

    private void handleDeltaProduce(RoutingContext routingContext) {
        HttpServerResponse resp = routingContext.response();

        LOGGER.info("Delta production requested via /deltaproduce endpoint");

        // Call the producer method - event loop guarantees serial execution
        this.produceDeltasOnDemand()
                .onSuccess(result -> {
                    LOGGER.info("Delta production completed successfully: {}", result.encode());
                    resp.setStatusCode(200)
                            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                            .end(result.encode());
                })
                .onFailure(error -> {
                    LOGGER.error("Delta production failed", error);
                    resp.setStatusCode(500)
                            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                            .end(new JsonObject()
                                    .put("status", "error")
                                    .put("message", error.getMessage())
                                    .encode());
                });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        LOGGER.info("Stopping SQS Log Producer...");
        this.shutdownInProgress = true;

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
    }

    /**
     * Produce delta files from SQS queue on-demand.
     * Processes 5-minute batches of messages until the queue is empty.
     * Serial execution is guaranteed by Vert.x event loop - no manual synchronization needed.
     * @return Future<JsonObject> with status information
     */
    private Future<JsonObject> produceDeltasOnDemand() {
        if (this.shutdownInProgress) {
            return Future.failedFuture("Producer is shutting down");
        }

        Promise<JsonObject> promise = Promise.promise();

        vertx.executeBlocking(blockingPromise -> {
            try {
                JsonObject result = new JsonObject();

                LOGGER.info("Starting on-demand delta production from SQS queue");

                // Receive all available messages from SQS (up to 10000)
                List<Message> allMessages = SqsMessageOperations.receiveAllAvailableMessages(
                        this.sqsClient, this.queueUrl, this.maxMessagesPerPoll, 
                        this.visibilityTimeout, 1000); // process at most 10000 optout requests per optout API call

                if (allMessages.isEmpty()) {
                    LOGGER.info("No messages in queue");
                    result.put("status", "success");
                    result.put("deltas_produced", 0);
                    result.put("entries_processed", 0);
                    blockingPromise.complete(result);
                    return;
                }

                LOGGER.info("Received {} messages from SQS", allMessages.size());

                // Parse and sort messages by timestamp
                List<SqsParsedMessage> parsedMessages = SqsMessageParser.parseAndSortMessages(allMessages);

                if (parsedMessages.isEmpty()) {
                    LOGGER.warn("No valid messages after parsing");
                    result.put("status", "success");
                    result.put("deltas_produced", 0);
                    result.put("entries_processed", 0);
                    blockingPromise.complete(result);
                    return;
                }

                // Filter messages: only process those where 5 minutes have elapsed since their timestamp vs current "now" time
                long currentTime = OptOutUtils.nowEpochSeconds();
                List<SqsParsedMessage> eligibleMessages = SqsMessageParser.filterEligibleMessages(
                        parsedMessages, this.deltaWindowSeconds, currentTime);

                if (eligibleMessages.isEmpty()) {
                    LOGGER.info("All {} messages are too recent (< {}s old), skipping processing", 
                        parsedMessages.size(), this.deltaWindowSeconds);
                    result.put("status", "skipped");
                    result.put("reason", "All messages too recent");
                    result.put("deltas_produced", 0);
                    result.put("entries_processed", 0);
                    blockingPromise.complete(result);
                    return;
                }

                if (eligibleMessages.size() < parsedMessages.size()) {
                    LOGGER.info("Filtered out {} too-recent messages, processing {} eligible messages", 
                        parsedMessages.size() - eligibleMessages.size(), eligibleMessages.size());
                }

                // Process eligible messages in 5-minute batches (based on message timestamp)
                DeltaProductionResult deltaResult = this.produceBatchedDeltas(eligibleMessages);

                result.put("status", "success");
                result.put("deltas_produced", deltaResult.getDeltasProduced());
                result.put("entries_processed", deltaResult.getEntriesProcessed());
                LOGGER.info("Delta production complete: {} deltas, {} entries", 
                    deltaResult.getDeltasProduced(), deltaResult.getEntriesProcessed());

                blockingPromise.complete(result);

            } catch (Exception e) {
                LOGGER.error("Error in on-demand delta production", e);
                blockingPromise.fail(e);
            }
        }, promise);

        return promise.future();
    }


    private DeltaProductionResult produceBatchedDeltas(List<SqsParsedMessage> messages) throws IOException {
        int deltasProduced = 0;
        int entriesProcessed = 0;

        ByteArrayOutputStream currentDeltaStream = null;
        String currentDeltaName = null;
        Long currentDeltaWindowStart = null;
        List<Message> currentDeltaMessages = new ArrayList<>();

        // Group messages into 5-minute windows and produce deltas
        for (SqsParsedMessage parsed : messages) {
            // Check if we need to start a new delta based on the message timestamp
            boolean needNewDelta = false;

            if (currentDeltaWindowStart == null) {
                needNewDelta = true;
            } else {
                long windowEnd = currentDeltaWindowStart + this.deltaWindowSeconds;
                if (parsed.getTimestamp() >= windowEnd) {
                    // Upload current delta
                    if (currentDeltaStream != null) {
                        this.uploadDeltaAndDeleteMessages(currentDeltaStream, currentDeltaName, currentDeltaWindowStart, currentDeltaMessages);
                        deltasProduced++;
                        currentDeltaMessages.clear();
                    }
                    needNewDelta = true;
                }
            }

            if (needNewDelta) {
                // Start a new delta for this time window (round down to the nearest window boundary)
                currentDeltaWindowStart = (parsed.getTimestamp() / this.deltaWindowSeconds) * this.deltaWindowSeconds;
                currentDeltaName = OptOutUtils.newDeltaFileName(this.replicaId);
                currentDeltaStream = new ByteArrayOutputStream();
                
                this.writeStartOfDelta(currentDeltaStream, currentDeltaWindowStart);

                LOGGER.info("Started new delta: {} for time window [{}, {})",
                    currentDeltaName, currentDeltaWindowStart, currentDeltaWindowStart + this.deltaWindowSeconds);
            }

            // Write entry to current delta
            this.writeOptOutEntry(currentDeltaStream, parsed.getHashBytes(), parsed.getIdBytes(), parsed.getTimestamp());

            // Track this message with the current delta
            currentDeltaMessages.add(parsed.getOriginalMessage());
            entriesProcessed++;
        }

        // Upload the last delta if any
        if (currentDeltaStream != null && !currentDeltaMessages.isEmpty()) {
            this.uploadDeltaAndDeleteMessages(currentDeltaStream, currentDeltaName, currentDeltaWindowStart, currentDeltaMessages);
            deltasProduced++;
        }

        return new DeltaProductionResult(deltasProduced, entriesProcessed);
    }

    /**
     * Writes the start-of-delta entry with null hash and window start timestamp.
     */
    private void writeStartOfDelta(ByteArrayOutputStream stream, long windowStart) throws IOException {
        LOGGER.debug("writeStartOfDelta: windowStart={}, buffer pos={} limit={} cap={}", 
            windowStart, buffer.position(), buffer.limit(), buffer.capacity());
        
        this.checkBufferSize(OptOutConst.EntrySize);
        
        LOGGER.debug("After checkBufferSize: buffer pos={} limit={} cap={}", 
            buffer.position(), buffer.limit(), buffer.capacity());
        
        buffer.put(OptOutUtils.nullHashBytes);
        buffer.put(OptOutUtils.nullHashBytes);
        buffer.putLong(windowStart);
        
        LOGGER.debug("After writes: buffer pos={}", buffer.position());
        
        buffer.flip();
        byte[] entry = new byte[buffer.remaining()];
        buffer.get(entry);
        
        LOGGER.debug("Writing {} bytes to stream, first 8 bytes of timestamp: {}", 
            entry.length, String.format("%02x %02x %02x %02x %02x %02x %02x %02x", 
            entry[32], entry[33], entry[34], entry[35], entry[36], entry[37], entry[38], entry[39]));
        
        stream.write(entry);
        buffer.clear();
    }

    /**
     * Writes a single opt-out entry to the delta stream.
     */
    private void writeOptOutEntry(ByteArrayOutputStream stream, byte[] hashBytes, byte[] idBytes, long timestamp) throws IOException {
        this.checkBufferSize(OptOutConst.EntrySize);
        OptOutEntry.writeTo(buffer, hashBytes, idBytes, timestamp);
        buffer.flip();
        byte[] entry = new byte[buffer.remaining()];
        buffer.get(entry);
        stream.write(entry);
        buffer.clear();
    }

    /**
     * Writes the end-of-delta sentinel entry with ones hash and window end timestamp.
     */
    private void writeEndOfDelta(ByteArrayOutputStream stream, long windowEnd) throws IOException {
        this.checkBufferSize(OptOutConst.EntrySize);
        buffer.put(OptOutUtils.onesHashBytes);
        buffer.put(OptOutUtils.onesHashBytes);
        buffer.putLong(windowEnd);
        buffer.flip();
        byte[] entry = new byte[buffer.remaining()];
        buffer.get(entry);
        stream.write(entry);
        buffer.clear();
    }



    // Upload a delta to S3 and delete messages from SQS after successful upload
    private void uploadDeltaAndDeleteMessages(ByteArrayOutputStream deltaStream, String deltaName, Long windowStart, List<Message> messages) throws IOException {
        try {
            // Add end-of-delta entry
            long endTimestamp = windowStart + this.deltaWindowSeconds;
            this.writeEndOfDelta(deltaStream, endTimestamp);

            // upload
            byte[] deltaData = deltaStream.toByteArray();
            String s3Path = this.cloudSync.toCloudPath(deltaName);

            LOGGER.info("SQS Delta Upload - fileName: {}, s3Path: {}, size: {} bytes, messages: {}, window: [{}, {})",
                deltaName, s3Path, deltaData.length, messages.size(), windowStart, endTimestamp);

            boolean uploadSucceeded = false;
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(deltaData)) {
                this.cloudStorage.upload(inputStream, s3Path);
                LOGGER.info("Successfully uploaded delta to S3: {}", s3Path);
                uploadSucceeded = true;

                // publish event
                this.publishDeltaProducedEvent(deltaName);
                this.counterDeltaProduced.increment();
                this.counterEntriesProcessed.increment(messages.size());

            } catch (Exception uploadEx) {
                LOGGER.error("Failed to upload delta to S3: " + uploadEx.getMessage(), uploadEx);
                throw new IOException("S3 upload failed", uploadEx);
            }

            // CRITICAL: Only delete messages from SQS after successful S3 upload
            if (uploadSucceeded && !messages.isEmpty()) {
                LOGGER.info("Deleting {} messages from SQS after successful S3 upload", messages.size());
                SqsMessageOperations.deleteMessagesFromSqs(this.sqsClient, this.queueUrl, messages);
            }

            // Close the stream
            deltaStream.close();

        } catch (Exception ex) {
            LOGGER.error("Error uploading delta: " + ex.getMessage(), ex);
            throw new IOException("Delta upload failed", ex);
        }
    }

    private void publishDeltaProducedEvent(String newDelta) {
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
