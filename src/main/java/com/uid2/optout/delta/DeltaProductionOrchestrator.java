package com.uid2.optout.delta;

import com.uid2.optout.sqs.SqsMessageOperations;
import com.uid2.optout.sqs.SqsParsedMessage;
import com.uid2.optout.sqs.SqsWindowReader;
import com.uid2.optout.vertx.OptOutTrafficCalculator;
import com.uid2.optout.vertx.OptOutTrafficFilter;
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
    private final DeltaUploadService deltaUploadService;
    private final DeltaUploadService droppedRequestUploadService;
    private final DeltaManualOverrideService manualOverrideService;
    private final OptOutTrafficFilter trafficFilter;
    private final OptOutTrafficCalculator trafficCalculator;
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
            DeltaUploadService deltaUploadService,
            DeltaUploadService droppedRequestUploadService,
            DeltaManualOverrideService manualOverrideService,
            OptOutTrafficFilter trafficFilter,
            OptOutTrafficCalculator trafficCalculator,
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
     * @param onDeltaProduced Called with delta filename after each successful delta upload (for event publishing)
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
        List<Message> deltaMessages = new ArrayList<>();
        List<Message> droppedMessages = new ArrayList<>();
        
        for (SqsParsedMessage msg : messages) {
            if (trafficFilter.isDenylisted(msg)) {
                writeDroppedRequestEntry(droppedRequestStream, msg);
                droppedMessages.add(msg.getOriginalMessage());
            } else {
                deltaFileWriter.writeOptOutEntry(deltaStream, msg.getHashBytes(), msg.getIdBytes(), msg.getTimestamp());
                deltaMessages.add(msg.getOriginalMessage());
            }
        }

        // check traffic calculator
        SqsMessageOperations.QueueAttributes queueAttributes = SqsMessageOperations.getQueueAttributes(this.sqsClient, this.queueUrl);
        OptOutTrafficCalculator.TrafficStatus trafficStatus = this.trafficCalculator.calculateStatus(queueAttributes);
        
        if (trafficStatus == OptOutTrafficCalculator.TrafficStatus.DELAYED_PROCESSING) {
            LOGGER.error("optout delta production has hit DELAYED_PROCESSING status, stopping production and setting manual override");
            manualOverrideService.setDelayedProcessing();
            return true;
        }

        // upload delta file if there are non-denylisted messages
        if (!deltaMessages.isEmpty()) {
            uploadDelta(deltaStream, deltaName, windowStart, deltaMessages, onDeltaProduced);
            result.incrementDeltasProduced();
            result.incrementEntriesProcessed(deltaMessages.size());
        }

        // upload dropped request file if there are denylisted messages
        if (!droppedMessages.isEmpty() && droppedRequestUploadService != null) {
            uploadDroppedRequests(droppedRequestStream, droppedRequestName, windowStart, droppedMessages);
            result.incrementDroppedRequestFilesProduced();
            result.incrementDroppedRequestsProcessed(droppedMessages.size());
        }

        LOGGER.info("processed window [{}, {}]: {} entries, {} dropped requests",
                windowStart, windowStart + this.deltaWindowSeconds,
                deltaMessages.size(), droppedMessages.size());

        return false;
    }

    private void uploadDelta(ByteArrayOutputStream deltaStream, String deltaName, 
                             long windowStart, List<Message> messages, 
                             Consumer<String> onDeltaProduced) throws IOException {
        // add end-of-delta entry
        long endTimestamp = windowStart + this.deltaWindowSeconds;
        deltaFileWriter.writeEndOfDelta(deltaStream, endTimestamp);

        byte[] deltaData = deltaStream.toByteArray();
        String s3Path = this.cloudSync.toCloudPath(deltaName);

        deltaUploadService.uploadAndDeleteMessages(deltaData, s3Path, messages, (count) -> {
            metrics.recordDeltaProduced(count);
            onDeltaProduced.accept(deltaName);
        });
    }

    private void uploadDroppedRequests(JsonArray droppedRequestStream, String droppedRequestName,
                                       long windowStart, List<Message> messages) throws IOException {
        byte[] droppedRequestData = droppedRequestStream.encode().getBytes();

        droppedRequestUploadService.uploadAndDeleteMessages(droppedRequestData, droppedRequestName, messages, 
                metrics::recordDroppedRequestsProduced);
    }

    /**
     * Writes a dropped request entry to the JSON array.
     */
    private void writeDroppedRequestEntry(JsonArray droppedRequestArray, SqsParsedMessage parsed) {
        String messageBody = parsed.getOriginalMessage().body();
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
            LOGGER.error("delta production job has been running for {} seconds", elapsedTime);
        }
        
        if (elapsedTime > this.jobTimeoutSeconds) {
            LOGGER.error("delta production job has been running for {} seconds (exceeds timeout of {}s)",
                    elapsedTime, this.jobTimeoutSeconds);
            return true;
        }
        return false;
    }
}

