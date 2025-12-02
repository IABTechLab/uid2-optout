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
import java.util.concurrent.atomic.AtomicReference;

/**
 * SQS-based opt-out log producer that creates delta files asynchronously.
 * 
 * <h2>Async Job Processing</h2>
 * The /optout/deltaproduce endpoint starts jobs asynchronously and returns immediately with HTTP 202.
 * Jobs run on worker threads via {@link Vertx#executeBlocking(java.util.concurrent.Callable)} and can
 * take some time to complete. Clients should poll GET /optout/deltaproduce/status to check progress.
 * 
 * <h2>Mutual Exclusion (Per-Pod)</h2>
 * Each pod prevents concurrent jobs using {@link AtomicReference#compareAndSet}.
 * Only ONE job can run per pod at a time. Attempting to start a second job returns HTTP 409 Conflict.
 * 
 * <h2>Job Lifecycle and Auto-Clearing</h2>
 * Completed or failed jobs are automatically cleared when a new POST request is made.
 * <ul>
 *   <li><strong>Running jobs</strong> - Cannot be replaced; POST returns 409 Conflict</li>
 *   <li><strong>Completed/Failed jobs</strong> - Automatically replaced by new jobs on POST</li>
 *   <li><strong>Status polling</strong> - GET endpoint shows current or most recent job status</li>
 * </ul>
 * 
 * <h2>Kubernetes Deployment with Session Affinity</h2>
 * In K8s with multiple pods, each pod maintains its own independent job state. To ensure requests
 * from the same client (e.g., a cronjob) consistently hit the same pod for job creation and status
 * polling, the Service must be configured with session affinity:
 * <pre>
 * sessionAffinity: ClientIP
 * sessionAffinityConfig:
 *   clientIP:
 *     timeoutSeconds: 10800  # 3 hours, adjust based on job duration
 * </pre>
 * 
 * <p>This ensures all requests from the same source IP are routed to the same pod, allowing proper
 * job lifecycle management (POST to start, GET to poll)</p>
 * 
 * <p><strong>Note:</strong> SQS visibility timeout provides natural coordination across pods,
 * limiting duplicate message processing even if multiple pods run jobs concurrently.</p>
 * 
 * <h2>API Endpoints</h2>
 * <ul>
 *   <li><code>POST /optout/deltaproduce</code> - Start async job, auto-clears completed/failed jobs (returns 202 Accepted or 409 Conflict if running)</li>
 *   <li><code>GET /optout/deltaproduce/status</code> - Poll job status (returns running/completed/failed/idle)</li>
 * </ul>
 */
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
    private final int jobTimeoutSeconds;
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
    
    //Tracks the current delta production job status for this pod.
    private final AtomicReference<DeltaProduceJobStatus> currentJob = new AtomicReference<>(null);
    
    // Helper for reading complete 5-minute windows from SQS
    private final SqsWindowReader windowReader;

    public OptOutSqsLogProducer(JsonObject jsonConfig, ICloudStorage cloudStorage, OptOutCloudSync cloudSync) throws IOException {
        this(jsonConfig, cloudStorage, cloudSync, Const.Event.DeltaProduce);
    }

    public OptOutSqsLogProducer(JsonObject jsonConfig, ICloudStorage cloudStorage, OptOutCloudSync cloudSync, String eventDeltaProduced) throws IOException {
        this(jsonConfig, cloudStorage, cloudSync, eventDeltaProduced, null);
    }

    // Constructor for testing - allows injecting mock SqsClient
    public OptOutSqsLogProducer(JsonObject jsonConfig, ICloudStorage cloudStorage, OptOutCloudSync cloudSync, String eventDeltaProduced, SqsClient sqsClient) throws IOException {
        this.eventDeltaProduced = eventDeltaProduced;
        this.replicaId = OptOutUtils.getReplicaId(jsonConfig);
        this.cloudStorage = cloudStorage;
        this.cloudSync = cloudSync;

        // Initialize SQS client
        this.queueUrl = jsonConfig.getString(Const.Config.OptOutSqsQueueUrlProp);
        if (this.queueUrl == null || this.queueUrl.isEmpty()) {
            throw new IOException("SQS queue URL not configured");
        }

        // Use injected client for testing, or create new one
        this.sqsClient = sqsClient != null ? sqsClient : SqsClient.builder().build();
        LOGGER.info("SQS client initialized for queue: " + this.queueUrl);

        // SQS Configuration
        this.maxMessagesPerPoll = 10; // SQS max is 10
        this.visibilityTimeout = jsonConfig.getInteger(Const.Config.OptOutSqsVisibilityTimeoutProp, 240); // 4 minutes default
        this.deltaWindowSeconds = 300; // Fixed 5 minutes for all deltas
        this.jobTimeoutSeconds = jsonConfig.getInteger(Const.Config.OptOutDeltaJobTimeoutSecondsProp, 10800); // 3 hours default

        // HTTP server configuration - use port offset + 1 to avoid conflicts
        this.listenPort = Const.Port.ServicePortForOptOut + Utils.getPortOffset() + 1;
        
        // Authentication
        this.internalApiKey = jsonConfig.getString(Const.Config.OptOutInternalApiTokenProp);
        this.internalAuth = new InternalAuthMiddleware(this.internalApiKey, "optout-sqs");

        int bufferSize = jsonConfig.getInteger(Const.Config.OptOutProducerBufferSizeProp);
        this.buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
        
        // Initialize window reader
        this.windowReader = new SqsWindowReader(
            this.sqsClient, this.queueUrl, this.maxMessagesPerPoll, 
            this.visibilityTimeout, this.deltaWindowSeconds
        );
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

    private Router createRouter() {
        Router router = Router.router(vertx);
        
        // POST endpoint to start delta production job (async, returns immediately)
        router.post(Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .handler(internalAuth.handleWithAudit(this::handleDeltaProduceStart));
        
        // GET endpoint to poll job status
        router.get(Endpoints.OPTOUT_DELTA_PRODUCE.toString() + "/status")
                .handler(internalAuth.handleWithAudit(this::handleDeltaProduceStatus));
        
        return router;
    }


    /**
     * Handler for GET /optout/deltaproduce/status
     * Returns the status of the current or most recent delta production job on this pod
     */
    private void handleDeltaProduceStatus(RoutingContext routingContext) {
        HttpServerResponse resp = routingContext.response();

        DeltaProduceJobStatus job = currentJob.get();
        
        if (job == null) {
            resp.setStatusCode(200)
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .end(new JsonObject()
                            .put("state", "idle")
                            .put("message", "No job running on this pod")
                            .encode());
            return;
        }

        resp.setStatusCode(200)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .end(job.toJson().encode());
    }

    /**
     * Handler for POST /optout/deltaproduce
     * Starts an async delta production job and returns immediately.
     * 
     * <p><strong>Auto-clearing behavior:</strong>
     * <ul>
     *   <li>If no job exists or previous job is completed/failed: Starts new job immediately</li>
     *   <li>If a job is currently running: Returns 409 Conflict (cannot replace running jobs)</li>
     * </ul>
     * 
     */
    private void handleDeltaProduceStart(RoutingContext routingContext) {
        HttpServerResponse resp = routingContext.response();

        LOGGER.info("Delta production job requested via /deltaproduce endpoint");

        
        DeltaProduceJobStatus existingJob = currentJob.get();
        
        // If there's an existing job, check if it's still running
        if (existingJob != null) {
            if (existingJob.getState() == DeltaProduceJobStatus.JobState.RUNNING) {
                // Cannot replace a running job - 409 Conflict
                LOGGER.warn("Delta production job already running on this pod");
                resp.setStatusCode(409)
                        .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                        .end(new JsonObject()
                                .put("status", "conflict")
                                .put("message", "A delta production job is already running on this pod")
                                .put("current_job", existingJob.toJson())
                                .encode());
                return;
            }
            
            LOGGER.info("Auto-clearing previous {} job to start new one", existingJob.getState());
        }

        DeltaProduceJobStatus newJob = new DeltaProduceJobStatus();

        // Try to set the new job
        if (!currentJob.compareAndSet(existingJob, newJob)) {
            resp.setStatusCode(409)
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .end(new JsonObject()
                            .put("status", "conflict")
                            .put("message", "Job state changed, please retry")
                            .encode());
            return;
        }

        // Start the job asynchronously
        LOGGER.info("Starting delta production job");
        this.startDeltaProductionJob(newJob);

        // Return immediately with 202 Accepted
        resp.setStatusCode(202)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .end(new JsonObject()
                        .put("status", "accepted")
                        .put("message", "Delta production job started on this pod")
                        .encode());
    }

    /**
     * Starts the delta production job asynchronously
     * The job runs on a worker thread and updates the DeltaProduceJobStatus when complete
     */
    private void startDeltaProductionJob(DeltaProduceJobStatus job) {
        vertx.executeBlocking(() -> {
            LOGGER.info("Executing delta production job");
            return produceDeltasBlocking();
        }).onComplete(ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result();
                job.complete(result);
                LOGGER.info("Delta production job succeeded: {}", result.encode());
            } else {
                String errorMsg = ar.cause().getMessage();
                job.fail(errorMsg);
                LOGGER.error("Delta production job failed: {}", errorMsg, ar.cause());
            }
        });
    }

    /**
     * Produce delta files from SQS queue (blocking operation).
     * Reads messages from queue and processes them in 5-minute windows.
     * Continues until queue is empty or all remaining messages are too recent.
     * This method should be called from a worker thread via executeBlocking.
     * @return JsonObject with status information
     * @throws Exception if production fails
     */
    private JsonObject produceDeltasBlocking() throws Exception {
        if (this.shutdownInProgress) {
            throw new Exception("Producer is shutting down");
        }

        JsonObject result = new JsonObject();
        LOGGER.info("Starting delta production from SQS queue");

        // Process messages until queue is empty or messages are too recent
        DeltaProductionResult deltaResult = this.produceBatchedDeltas();

        // Determine status based on results
        if (deltaResult.getDeltasProduced() == 0 && deltaResult.stoppedDueToRecentMessages()) {
            // No deltas produced because all messages were too recent
            result.put("status", "skipped");
            result.put("reason", "All messages too recent");
            LOGGER.info("Delta production skipped: all messages too recent");
        } else {
            result.put("status", "success");
            LOGGER.info("Delta production complete: {} deltas, {} entries", 
                deltaResult.getDeltasProduced(), deltaResult.getEntriesProcessed());
        }
        
        result.put("deltas_produced", deltaResult.getDeltasProduced());
        result.put("entries_processed", deltaResult.getEntriesProcessed());

        return result;
    }


    /**
     * Reads messages from SQS and produces delta files in 5 minute batches.
     * Continues until queue is empty or messages are too recent.
     * 
     * @return DeltaProductionResult with counts and stop reason
     * @throws IOException if delta production fails
     */
    private DeltaProductionResult produceBatchedDeltas() throws IOException {
        int deltasProduced = 0;
        int totalEntriesProcessed = 0;
        boolean stoppedDueToRecentMessages = false;
        
        long jobStartTime = OptOutUtils.nowEpochSeconds();
        LOGGER.info("Starting delta production from SQS queue");

        // Read and process windows until done
        while (true) {
            if(checkJobTimeout(jobStartTime)){
                break;
            }
            
            // Read one complete 5-minute window
            SqsWindowReader.WindowReadResult windowResult = windowReader.readWindow();
            
            // If no messages, we're done (queue empty or messages too recent)
            if (windowResult.isEmpty()) {
                stoppedDueToRecentMessages = windowResult.stoppedDueToRecentMessages();
                LOGGER.info("Delta production complete - no more eligible messages");
                break;
            }
            
            // Produce delta for this window
            long windowStart = windowResult.getWindowStart();
            List<SqsParsedMessage> messages = windowResult.getMessages();
            
            // Create delta file
            String deltaName = OptOutUtils.newDeltaFileName(this.replicaId);
            ByteArrayOutputStream deltaStream = new ByteArrayOutputStream();
            writeStartOfDelta(deltaStream, windowStart);
            
            // Write all messages
            List<Message> sqsMessages = new ArrayList<>();
            for (SqsParsedMessage msg : messages) {
                writeOptOutEntry(deltaStream, msg.getHashBytes(), msg.getIdBytes(), msg.getTimestamp());
                sqsMessages.add(msg.getOriginalMessage());
            }
            
            // Upload and delete
            uploadDeltaAndDeleteMessages(deltaStream, deltaName, windowStart, sqsMessages);
            deltasProduced++;
            totalEntriesProcessed += messages.size();
            
            LOGGER.info("Produced delta for window [{}, {}] with {} messages",
                windowStart, windowStart + this.deltaWindowSeconds, messages.size());
        }

        long totalDuration = OptOutUtils.nowEpochSeconds() - jobStartTime;
        LOGGER.info("Delta production complete: took {}s, produced {} deltas, processed {} entries",
            totalDuration, deltasProduced, totalEntriesProcessed);

        return new DeltaProductionResult(deltasProduced, totalEntriesProcessed, stoppedDueToRecentMessages);
    }

    /**
     * Checks if job has exceeded timeout
     */
    private boolean checkJobTimeout(long jobStartTime) {
        long elapsedTime = OptOutUtils.nowEpochSeconds() - jobStartTime;
        if (elapsedTime > 3600) { // 1 hour - log warning message
            LOGGER.error("Delta production job has been running for {} seconds",
                elapsedTime);
            return false;
        }
        if (elapsedTime > this.jobTimeoutSeconds) {
            LOGGER.error("Delta production job has been running for {} seconds (exceeds timeout of {}s)",
                elapsedTime, this.jobTimeoutSeconds);
            return true; // deadline exceeded
        }
        return false;
    }

    /**
     * Writes the start-of-delta entry with null hash and window start timestamp.
     */
    private void writeStartOfDelta(ByteArrayOutputStream stream, long windowStart) throws IOException {
        
        this.checkBufferSize(OptOutConst.EntrySize);
        
        buffer.put(OptOutUtils.nullHashBytes);
        buffer.put(OptOutUtils.nullHashBytes);
        buffer.putLong(windowStart);
        
        buffer.flip();
        byte[] entry = new byte[buffer.remaining()];
        buffer.get(entry);
        
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
