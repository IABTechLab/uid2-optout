package com.uid2.optout.delta;

import com.uid2.optout.sqs.SqsMessageOperations;
import com.uid2.optout.sqs.SqsParsedMessage;
import com.uid2.optout.sqs.SqsWindowReader;
import com.uid2.optout.traffic.TrafficCalculator;
import com.uid2.optout.traffic.TrafficCalculator.TrafficStatus;
import com.uid2.optout.traffic.TrafficFilter;
import com.uid2.shared.optout.OptOutCloudSync;
import com.uid2.shared.optout.OptOutUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Orchestrates the delta production workflow.
 * 
 * <p>This class encapsulates the core delta production logic:</p>
 * <ul>
 *   <li>Reading messages from SQS in 5-minute windows</li>
 *   <li>Filtering denylisted messages</li>
 *   <li>Checking circuit breakers (manual override, traffic calculator)</li>
 *   <li>Constructing delta files and dropped request files</li>
 *   <li>Uploading to S3 and deleting processed messages</li>
 * </ul>
 * 
 */
public class DeltaProductionOrchestrator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeltaProductionOrchestrator.class);

    private final SqsClient sqsClient;
    private final String queueUrl;
    private final int replicaId;
    private final int deltaWindowSeconds;
    private final int jobTimeoutSeconds;
    
    private final SqsWindowReader windowReader;
    private final DeltaFileWriter deltaFileWriter;
    private final S3UploadService deltaUploadService;
    private final S3UploadService droppedRequestUploadService;
    private final ManualOverrideService manualOverrideService;
    private final TrafficFilter trafficFilter;
    private final TrafficCalculator trafficCalculator;
    private final OptOutCloudSync cloudSync;
    private final DeltaProductionMetrics metrics;

    public DeltaProductionOrchestrator(
            SqsClient sqsClient,
            String queueUrl,
            int replicaId,
            int deltaWindowSeconds,
            int jobTimeoutSeconds,
            SqsWindowReader windowReader,
            DeltaFileWriter deltaFileWriter,
            S3UploadService deltaUploadService,
            S3UploadService droppedRequestUploadService,
            ManualOverrideService manualOverrideService,
            TrafficFilter trafficFilter,
            TrafficCalculator trafficCalculator,
            OptOutCloudSync cloudSync,
            DeltaProductionMetrics metrics) {
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.replicaId = replicaId;
        this.deltaWindowSeconds = deltaWindowSeconds;
        this.jobTimeoutSeconds = jobTimeoutSeconds;
        this.windowReader = windowReader;
        this.deltaFileWriter = deltaFileWriter;
        this.deltaUploadService = deltaUploadService;
        this.droppedRequestUploadService = droppedRequestUploadService;
        this.manualOverrideService = manualOverrideService;
        this.trafficFilter = trafficFilter;
        this.trafficCalculator = trafficCalculator;
        this.cloudSync = cloudSync;
        this.metrics = metrics;
    }

    /**
     * Produces delta files from SQS queue in batched 5-minute windows.
     * 
     * Continues until queue is empty, messages are too recent, circuit breaker is triggered, or job timeout is reached.
     * 
     * @param onDeltaProduced Called with delta filename after each successful delta upload (for event & metrics publishing)
     * @return DeltaProductionResult with production statistics
     * @throws IOException if delta production fails
     */
    public DeltaProductionResult produceBatchedDeltas(Consumer<String> onDeltaProduced) throws IOException {
        
        // check for manual override
        if (manualOverrideService.isDelayedProcessing()) {
            LOGGER.info("manual override set to DELAYED_PROCESSING, skipping production");
            return DeltaProductionResult.builder().stopReason(StopReason.MANUAL_OVERRIDE_ACTIVE).build();
        }

        DeltaProductionResult.Builder result = DeltaProductionResult.builder();
        long jobStartTime = OptOutUtils.nowEpochSeconds();
        
        LOGGER.info("starting delta production from SQS queue (replicaId: {}, deltaWindowSeconds: {}, jobTimeoutSeconds: {})", 
            this.replicaId, this.deltaWindowSeconds, this.jobTimeoutSeconds);

        // read and process windows until done
        while (!isJobTimedOut(jobStartTime)) {

            // read one complete 5-minute window
            SqsWindowReader.WindowReadResult windowResult = windowReader.readWindow();

            // if no messages, we're done (queue empty or messages too recent)
            if (windowResult.isEmpty()) {
                result.stopReason(windowResult.getStopReason());
                LOGGER.info("delta production complete - no more eligible messages (reason: {})", windowResult.getStopReason().name());
                break;
            }

            // process this window
            boolean isDelayedProcessing = processWindow(windowResult, result, onDeltaProduced);
            
            // circuit breaker triggered
            if (isDelayedProcessing) {
                result.stopReason(StopReason.CIRCUIT_BREAKER_TRIGGERED);
                return result.build();
            }
        }

        return result.build();
    }

    /**
     * Processes a single 5-minute window of messages.
     * 
     * @param windowResult The window data to process
     * @param result The builder to accumulate statistics into
     * @param onDeltaProduced Callback for when a delta is produced
     * @return true if the circuit breaker triggered
     */
    private boolean processWindow(SqsWindowReader.WindowReadResult windowResult,
                                  DeltaProductionResult.Builder result,
                                  Consumer<String> onDeltaProduced) throws IOException {
        long windowStart = windowResult.getWindowStart();
        List<SqsParsedMessage> messages = windowResult.getMessages();

        // check for manual override
        if (manualOverrideService.isDelayedProcessing()) {
            LOGGER.info("manual override set to DELAYED_PROCESSING, stopping production");
            return true;
        }

        // create buffers for current window
        ByteArrayOutputStream deltaStream = new ByteArrayOutputStream();
        JsonArray droppedRequestStream = new JsonArray();
        
        // get file names for current window
        String deltaName = OptOutUtils.newDeltaFileName(this.replicaId);
        String droppedRequestName = generateDroppedRequestFileName();

        // write start of delta
        deltaFileWriter.writeStartOfDelta(deltaStream, windowStart);

        // separate messages into delta entries and dropped requests
        List<SqsParsedMessage> deltaMessages = new ArrayList<>();
        List<SqsParsedMessage> droppedMessages = new ArrayList<>();
        
        for (SqsParsedMessage msg : messages) {
            if (trafficFilter.isDenylisted(msg)) {
                writeDroppedRequestEntry(droppedRequestStream, msg);
                droppedMessages.add(msg);
            } else {
                deltaFileWriter.writeOptOutEntry(deltaStream, msg.hashBytes(), msg.idBytes(), msg.timestamp());
                deltaMessages.add(msg);
            }
        }

        // check traffic calculator
        SqsMessageOperations.QueueAttributes queueAttributes = SqsMessageOperations.getQueueAttributes(this.sqsClient, this.queueUrl);
        TrafficStatus trafficStatus = this.trafficCalculator.calculateStatus(deltaMessages, queueAttributes, windowResult.getRawMessagesRead());
        
        if (trafficStatus == TrafficStatus.DELAYED_PROCESSING) {
            LOGGER.error("circuit_breaker_triggered: traffic spike detected, stopping production and setting manual override");
            manualOverrideService.setDelayedProcessing();
            return true;
        }

        // upload delta file if there are non-denylisted messages
        if (!deltaMessages.isEmpty()) {
            uploadDelta(deltaStream, deltaName, windowStart, deltaMessages, onDeltaProduced);
            result.incrementDeltas(deltaMessages.size());
        }

        // upload dropped request file if there are denylisted messages
        if (!droppedMessages.isEmpty() && droppedRequestUploadService != null) {
            uploadDroppedRequests(droppedRequestStream, droppedRequestName, windowStart, droppedMessages);
            result.incrementDroppedRequests(droppedMessages.size());
        }

        LOGGER.info("processed window [{}, {}]: {} entries, {} dropped requests",
                windowStart, windowStart + this.deltaWindowSeconds,
                deltaMessages.size(), droppedMessages.size());

        return false;
    }

    /**
     * Adds end-of-delta entry to delta stream and converts to byte array,
     * then uploads delta file to S3 and deletes associated messages from SQS.
     */
    private void uploadDelta(ByteArrayOutputStream deltaStream, String deltaName, 
                             long windowStart, List<SqsParsedMessage> messages, 
                             Consumer<String> onDeltaProduced) throws IOException {
        // add end-of-delta entry
        long endTimestamp = windowStart + this.deltaWindowSeconds;
        deltaFileWriter.writeEndOfDelta(deltaStream, endTimestamp);

        // convert delta stream to byte array
        byte[] deltaData = deltaStream.toByteArray();
        String s3Path = this.cloudSync.toCloudPath(deltaName);

        // get original messages for deletion
        List<Message> originalMessages = messages.stream().map(SqsParsedMessage::originalMessage).collect(Collectors.toList());

        // upload and delete
        deltaUploadService.uploadAndDeleteMessages(deltaData, s3Path, originalMessages, (count) -> {
            metrics.recordDeltaProduced(count);
            onDeltaProduced.accept(deltaName);
        });
    }

    /**
     * Uploads dropped requests to S3 and deletes associated messages from SQS.
     */
    private void uploadDroppedRequests(JsonArray droppedRequestStream, String droppedRequestName,
                                       long windowStart, List<SqsParsedMessage> messages) throws IOException {
        
        // convert dropped request stream to byte array
        byte[] droppedRequestData = droppedRequestStream.encode().getBytes();

        // get original messages for deletion
        List<Message> originalMessages = messages.stream().map(SqsParsedMessage::originalMessage).collect(Collectors.toList());

        // upload and delete
        droppedRequestUploadService.uploadAndDeleteMessages(droppedRequestData, droppedRequestName, originalMessages, 
                metrics::recordDroppedRequestsProduced);
    }

    /**
     * Writes a dropped request entry to the JSON array.
     */
    private void writeDroppedRequestEntry(JsonArray droppedRequestArray, SqsParsedMessage parsed) {
        String messageBody = parsed.originalMessage().body();
        JsonObject messageJson = new JsonObject(messageBody);
        droppedRequestArray.add(messageJson);
    }

    /**
     * Generates a unique filename for dropped requests.
     */
    private String generateDroppedRequestFileName() {
        return String.format("%s%03d_%s_%08x.json", 
                "optout-dropped-", 
                replicaId, 
                Instant.now().truncatedTo(ChronoUnit.SECONDS).toString().replace(':', '.'), 
                OptOutUtils.rand.nextInt());
    }

    /**
     * Checks if the job has exceeded its timeout.
     */
    private boolean isJobTimedOut(long jobStartTime) {
        long elapsedTime = OptOutUtils.nowEpochSeconds() - jobStartTime;
        
        if (elapsedTime > 3600) { // 1 hour - log warning
            LOGGER.error("delta_job_timeout: job has been running for {} seconds", elapsedTime);
        }
        
        if (elapsedTime > this.jobTimeoutSeconds) {
            LOGGER.error("delta_job_timeout: job exceeded timeout, running for {} seconds (timeout: {}s)",
                    elapsedTime, this.jobTimeoutSeconds);
            return true;
        }
        return false;
    }
}

