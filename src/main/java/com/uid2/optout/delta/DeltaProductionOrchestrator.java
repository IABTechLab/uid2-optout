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
 *   <li>Constructingdelta files and dropped request files</li>
 *   <li>Uploading to S3 and deleting processed messages</li>
 * </ul>
 * 
 * <p>The orchestrator is stateless and thread-safe.</p>
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
     * <p>Continues until queue is empty, messages are too recent, or a circuit breaker triggers.</p>
     * 
     * @param onDeltaProduced Called with delta filename after each successful delta upload (for event publishing)
     * @return DeltaProduceResult with production statistics
     * @throws IOException if delta production fails
     */
    public DeltaProductionResult produceBatchedDeltas(Consumer<String> onDeltaProduced) throws IOException {
        // Check for manual override at the start
        if (manualOverrideService.isDelayedProcessing()) {
            LOGGER.info("Manual override set to DELAYED_PROCESSING, skipping production");
            return DeltaProductionResult.empty(StopReason.MANUAL_OVERRIDE_ACTIVE);
        }

        ProductionStats stats = new ProductionStats();
        long jobStartTime = OptOutUtils.nowEpochSeconds();
        
        LOGGER.info("Starting delta production from SQS queue (replicaId: {}, deltaWindowSeconds: {}, jobTimeoutSeconds: {})", 
            this.replicaId, this.deltaWindowSeconds, this.jobTimeoutSeconds);

        // Read and process windows until done
        while (!isJobTimedOut(jobStartTime)) {

            // Read one complete 5-minute window
            SqsWindowReader.WindowReadResult windowResult = windowReader.readWindow();

            // If no messages, we're done (queue empty or messages too recent)
            if (windowResult.isEmpty()) {
                stats.stopReason = windowResult.getStopReason();
                LOGGER.info("Delta production complete - no more eligible messages (reason: {})", stats.stopReason);
                break;
            }

            // Process this window
            WindowProcessingResult windowProcessingResult = processWindow(windowResult, onDeltaProduced);
            
            // Check if we should stop (circuit breaker triggered)
            if (windowProcessingResult.shouldStop) {
                stats.merge(windowProcessingResult);
                stats.stopReason = StopReason.CIRCUIT_BREAKER_TRIGGERED;
                return stats.toResult();
            }
            
            stats.merge(windowProcessingResult);

            LOGGER.info("Processed window [{}, {}]: {} entries, {} dropped requests",
                    windowResult.getWindowStart(), 
                    windowResult.getWindowStart() + this.deltaWindowSeconds, 
                    windowProcessingResult.entriesProcessed,
                    windowProcessingResult.droppedRequestsProcessed);
        }

        long totalDuration = OptOutUtils.nowEpochSeconds() - jobStartTime;
        LOGGER.info("Delta production complete: took {}s, produced {} deltas, processed {} entries, " +
                        "produced {} dropped request files, processed {} dropped requests, stop reason: {}",
                totalDuration, stats.deltasProduced, stats.entriesProcessed, 
                stats.droppedRequestFilesProduced, stats.droppedRequestsProcessed, stats.stopReason);

        return stats.toResult();
    }

    /**
     * Processes a single 5-minute window of messages.
     */
    private WindowProcessingResult processWindow(SqsWindowReader.WindowReadResult windowResult, 
                                                  Consumer<String> onDeltaProduced) throws IOException {
        WindowProcessingResult result = new WindowProcessingResult();
        
        long windowStart = windowResult.getWindowStart();
        List<SqsParsedMessage> messages = windowResult.getMessages();

        // Create buffers
        ByteArrayOutputStream deltaStream = new ByteArrayOutputStream();
        JsonArray droppedRequestStream = new JsonArray();
        
        String deltaName = OptOutUtils.newDeltaFileName(this.replicaId);
        String droppedRequestName = generateDroppedRequestFileName();

        // Write start of delta
        deltaFileWriter.writeStartOfDelta(deltaStream, windowStart);

        // Separate messages into delta entries and dropped requests
        List<Message> deltaMessages = new ArrayList<>();
        List<Message> droppedMessages = new ArrayList<>();
        
        for (SqsParsedMessage msg : messages) {
            if (trafficFilter.isDenylisted(msg)) {
                writeDroppedRequestEntry(droppedRequestStream, msg);
                droppedMessages.add(msg.getOriginalMessage());
                result.droppedRequestsProcessed++;
            } else {
                deltaFileWriter.writeOptOutEntry(deltaStream, msg.getHashBytes(), msg.getIdBytes(), msg.getTimestamp());
                deltaMessages.add(msg.getOriginalMessage());
                result.entriesProcessed++;
            }
        }

        // Check for manual override
        if (manualOverrideService.isDelayedProcessing()) {
            LOGGER.info("Manual override set to DELAYED_PROCESSING, stopping production");
            result.shouldStop = true;
            return result;
        }

        // Check traffic calculator
        SqsMessageOperations.QueueAttributes queueAttributes = SqsMessageOperations.getQueueAttributes(this.sqsClient, this.queueUrl);
        OptOutTrafficCalculator.TrafficStatus trafficStatus = this.trafficCalculator.calculateStatus(queueAttributes);
        
        if (trafficStatus == OptOutTrafficCalculator.TrafficStatus.DELAYED_PROCESSING) {
            LOGGER.error("OptOut Delta Production has hit DELAYED_PROCESSING status, stopping production");
            manualOverrideService.setDelayedProcessing();
            result.shouldStop = true;
            return result;
        }

        // Upload delta file if there are non-denylisted messages
        if (!deltaMessages.isEmpty()) {
            uploadDelta(deltaStream, deltaName, windowStart, deltaMessages, onDeltaProduced);
            result.deltasProduced++;
        }

        // Upload dropped request file if there are denylisted messages
        if (!droppedMessages.isEmpty() && droppedRequestUploadService != null) {
            uploadDroppedRequests(droppedRequestStream, droppedRequestName, windowStart, droppedMessages);
            result.droppedRequestFilesProduced++;
        }

        deltaStream.close();
        return result;
    }

    /**
     * Uploads a delta file to S3 and deletes processed messages from SQS.
     */
    private void uploadDelta(ByteArrayOutputStream deltaStream, String deltaName, 
                             long windowStart, List<Message> messages, 
                             Consumer<String> onDeltaProduced) throws IOException {
        // Add end-of-delta entry
        long endTimestamp = windowStart + this.deltaWindowSeconds;
        deltaFileWriter.writeEndOfDelta(deltaStream, endTimestamp);

        byte[] deltaData = deltaStream.toByteArray();
        String s3Path = this.cloudSync.toCloudPath(deltaName);

        LOGGER.info("SQS Delta Upload - fileName: {}, s3Path: {}, size: {} bytes, messages: {}, window: [{}, {})",
                deltaName, s3Path, deltaData.length, messages.size(), windowStart, endTimestamp);

        deltaUploadService.uploadAndDeleteMessages(deltaData, s3Path, messages, (count) -> {
            metrics.recordDeltaProduced(count);
            onDeltaProduced.accept(deltaName);
        });
    }

    /**
     * Uploads dropped requests to S3 and deletes processed messages from SQS.
     */
    private void uploadDroppedRequests(JsonArray droppedRequestStream, String droppedRequestName,
                                       long windowStart, List<Message> messages) throws IOException {
        byte[] droppedRequestData = droppedRequestStream.encode().getBytes();

        LOGGER.info("SQS Dropped Requests Upload - fileName: {}, size: {} bytes, messages: {}, window: [{}, {})",
                droppedRequestName, droppedRequestData.length, messages.size(), 
                windowStart, windowStart + this.deltaWindowSeconds);

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
            LOGGER.error("Delta production job has been running for {} seconds", elapsedTime);
        }
        
        if (elapsedTime > this.jobTimeoutSeconds) {
            LOGGER.error("Delta production job has been running for {} seconds (exceeds timeout of {}s)",
                    elapsedTime, this.jobTimeoutSeconds);
            return true;
        }
        return false;
    }

    /**
     * Mutable class for tracking production statistics during a job.
     */
    private static class ProductionStats {
        int deltasProduced = 0;
        int entriesProcessed = 0;
        int droppedRequestFilesProduced = 0;
        int droppedRequestsProcessed = 0;
        StopReason stopReason = StopReason.NONE;

        ProductionStats merge(WindowProcessingResult windowResult) {
            this.deltasProduced += windowResult.deltasProduced;
            this.entriesProcessed += windowResult.entriesProcessed;
            this.droppedRequestFilesProduced += windowResult.droppedRequestFilesProduced;
            this.droppedRequestsProcessed += windowResult.droppedRequestsProcessed;
            return this;
        }

        DeltaProductionResult toResult() {
            return new DeltaProductionResult(deltasProduced, entriesProcessed, 
                    droppedRequestFilesProduced, droppedRequestsProcessed, stopReason);
        }
    }

    /**
     * Result of processing a single window.
     */
    private static class WindowProcessingResult {
        int deltasProduced = 0;
        int entriesProcessed = 0;
        int droppedRequestFilesProduced = 0;
        int droppedRequestsProcessed = 0;
        boolean shouldStop = false;
    }
}

