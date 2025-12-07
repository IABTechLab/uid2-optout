package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.auth.InternalAuthMiddleware;
import com.uid2.optout.delta.DeltaFileWriter;
import com.uid2.optout.delta.DeltaManualOverrideService;
import com.uid2.optout.delta.DeltaProductionJobStatus;
import com.uid2.optout.delta.DeltaProductionResult;
import com.uid2.optout.delta.DeltaProductionMetrics;
import com.uid2.optout.delta.DeltaProductionOrchestrator;
import com.uid2.optout.delta.DeltaUploadService;
import com.uid2.optout.delta.StopReason;
import com.uid2.optout.sqs.SqsWindowReader;
import com.uid2.optout.vertx.OptOutTrafficCalculator.MalformedTrafficCalcConfigException;
import com.uid2.optout.vertx.OptOutTrafficFilter.MalformedTrafficFilterConfigException;
import com.uid2.shared.Utils;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.health.HealthComponent;
import com.uid2.shared.health.HealthManager;
import com.uid2.shared.optout.OptOutCloudSync;
import com.uid2.shared.optout.OptOutUtils;

import io.vertx.core.*;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;

import static com.uid2.optout.util.HttpResponseHelper.*;

import java.io.IOException;
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
    private final String eventDeltaProduced;
    private final int listenPort;
    private final InternalAuthMiddleware internalAuth;
    private final OptOutTrafficFilter trafficFilter;
    private final OptOutTrafficCalculator trafficCalculator;
    private final DeltaProductionOrchestrator orchestrator;
    
    // Tracks the current delta production job status for this pod
    private final AtomicReference<DeltaProductionJobStatus> currentJob = new AtomicReference<>(null);
    
    private volatile boolean shutdownInProgress = false;

    public OptOutSqsLogProducer(JsonObject jsonConfig, ICloudStorage cloudStorage, ICloudStorage cloudStorageDroppedRequests, OptOutCloudSync cloudSync, String eventDeltaProduced, SqsClient sqsClient) throws IOException, MalformedTrafficCalcConfigException, MalformedTrafficFilterConfigException {
        this.eventDeltaProduced = eventDeltaProduced;
        
        // Initialize SQS client
        String queueUrl = jsonConfig.getString(Const.Config.OptOutSqsQueueUrlProp);
        if (queueUrl == null || queueUrl.isEmpty()) {
            throw new IOException("SQS queue URL not configured");
        }
        this.sqsClient = sqsClient != null ? sqsClient : SqsClient.builder().build();
        LOGGER.info("SQS client initialized for queue: {}", queueUrl);

        // HTTP server configuration
        this.listenPort = Const.Port.ServicePortForOptOut + Utils.getPortOffset() + 1;
        
        // Authentication
        String internalApiKey = jsonConfig.getString(Const.Config.OptOutInternalApiTokenProp);
        this.internalAuth = new InternalAuthMiddleware(internalApiKey, "optout-sqs");

        // Circuit breaker tools
        this.trafficFilter = new OptOutTrafficFilter(jsonConfig.getString(Const.Config.TrafficFilterConfigPathProp));
        this.trafficCalculator = new OptOutTrafficCalculator(cloudStorage, jsonConfig.getString(Const.Config.OptOutSqsS3FolderProp), jsonConfig.getString(Const.Config.TrafficCalcConfigPathProp));

        // Configuration values for orchestrator setup
        int replicaId = OptOutUtils.getReplicaId(jsonConfig);
        int maxMessagesPerPoll = 10; // SQS max is 10
        int deltaWindowSeconds = 300; // Fixed 5 minutes for all deltas
        int visibilityTimeout = jsonConfig.getInteger(Const.Config.OptOutSqsVisibilityTimeoutProp, 240);
        int jobTimeoutSeconds = jsonConfig.getInteger(Const.Config.OptOutDeltaJobTimeoutSecondsProp, 10800);
        int maxMessagesPerFile = jsonConfig.getInteger(Const.Config.OptOutMaxMessagesPerFileProp, 10000);
        int bufferSize = jsonConfig.getInteger(Const.Config.OptOutProducerBufferSizeProp);

        // Orchestrator setup
        DeltaFileWriter deltaFileWriter = new DeltaFileWriter(bufferSize);
        DeltaUploadService deltaUploadService = new DeltaUploadService(cloudStorage, this.sqsClient, queueUrl);
        DeltaUploadService droppedRequestUploadService = new DeltaUploadService(cloudStorageDroppedRequests, this.sqsClient, queueUrl) ;
        DeltaManualOverrideService manualOverrideService = new DeltaManualOverrideService(cloudStorage, jsonConfig.getString(Const.Config.ManualOverrideS3PathProp));
        SqsWindowReader windowReader = new SqsWindowReader(
            this.sqsClient, queueUrl, maxMessagesPerPoll, 
            visibilityTimeout, deltaWindowSeconds, maxMessagesPerFile
        );

        this.orchestrator = new DeltaProductionOrchestrator(
            this.sqsClient,
            queueUrl,
            replicaId,
            deltaWindowSeconds,
            jobTimeoutSeconds,
            windowReader,
            deltaFileWriter,
            deltaUploadService,
            droppedRequestUploadService,
            manualOverrideService,
            this.trafficFilter,
            this.trafficCalculator,
            cloudSync,
            new DeltaProductionMetrics()
        );
        
        LOGGER.info("OptOutSqsLogProducer initialized with maxMessagesPerFile: {}, maxMessagesPerPoll: {}, visibilityTimeout: {}, deltaWindowSeconds: {}, jobTimeoutSeconds: {}",
            maxMessagesPerFile, maxMessagesPerPoll, visibilityTimeout, deltaWindowSeconds, jobTimeoutSeconds);
    }

    @Override
    public void start(Promise<Void> startPromise) {
        LOGGER.info("Attempting to start SQS Log Producer HTTP server on port: {}", listenPort);

        try {
            vertx.createHttpServer()
                    .requestHandler(createRouter())
                    .listen(listenPort, result -> {
                        if (result.succeeded()) {
                            this.healthComponent.setHealthStatus(true);
                            LOGGER.info("SQS Log Producer HTTP server started on port: {}", listenPort);
                            startPromise.complete();
                        } else {
                            LOGGER.error("Failed to start SQS Log Producer HTTP server", result.cause());
                            this.healthComponent.setHealthStatus(false, result.cause().getMessage());
                            startPromise.fail(result.cause());
                        }
                    });

        } catch (Exception e) {
            LOGGER.error("Failed to start SQS Log Producer HTTP server", e);
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

        DeltaProductionJobStatus job = currentJob.get();
        
        if (job == null) {
            sendIdle(resp, "No job running on this pod");
            return;
        }

        sendSuccess(resp, job.toJson());
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

        try {
            this.trafficFilter.reloadTrafficFilterConfig();
        } catch (MalformedTrafficFilterConfigException e) {
            LOGGER.error("Error reloading traffic filter config: " + e.getMessage(), e);
            sendError(resp, e);
            return;
        }

        try {
            this.trafficCalculator.reloadTrafficCalcConfig();
        } catch (MalformedTrafficCalcConfigException e) {
            LOGGER.error("Error reloading traffic calculator config: " + e.getMessage(), e);
            sendError(resp, e);
            return;
        }

        DeltaProductionJobStatus existingJob = currentJob.get();
        
        // If there's an existing job, check if it's still running
        if (existingJob != null) {
            if (existingJob.getState() == DeltaProductionJobStatus.JobState.RUNNING) {
                // Cannot replace a running job - 409 Conflict
                LOGGER.warn("Delta production job already running on this pod");
                sendConflict(resp, "A delta production job is already running on this pod");
                return;
            }
            
            LOGGER.info("Auto-clearing previous {} job to start new one", existingJob.getState());
        }

        DeltaProductionJobStatus newJob = new DeltaProductionJobStatus();

        // Try to set the new job
        if (!currentJob.compareAndSet(existingJob, newJob)) {
            sendConflict(resp, "Job state changed, please retry");
            return;
        }

        // Start the job asynchronously
        LOGGER.info("New delta production job initialized");
        this.startDeltaProductionJob(newJob);

        // Return immediately with 202 Accepted
        sendAccepted(resp, "Delta production job started on this pod");
    }

    /**
     * Starts the delta production job asynchronously
     * The job runs on a worker thread and updates the DeltaProduceJobStatus when complete
     */
    private void startDeltaProductionJob(DeltaProductionJobStatus job) {
        vertx.executeBlocking(() -> {
            LOGGER.info("Delta production job starting on worker thread");
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
        LOGGER.info("Starting delta production from SQS queue");

        // Process messages until queue is empty or messages are too recent
        DeltaProductionResult deltaResult = this.produceBatchedDeltas();

        StopReason stopReason = deltaResult.getStopReason();
        boolean producedDeltas = deltaResult.getDeltasProduced() > 0;
        boolean producedDroppedRequests = deltaResult.getDroppedRequestFilesProduced() > 0;

        // Determine status based on results:
        // "success" = produced work OR completed normally
        // "skipped" = stopped early due to abnormal conditions (circuit breaker, override, too recent)
        boolean isSkipped = stopReason == StopReason.CIRCUIT_BREAKER_TRIGGERED
            || stopReason == StopReason.MESSAGES_TOO_RECENT
            || stopReason == StopReason.MANUAL_OVERRIDE_ACTIVE;
        
        boolean isSuccess = !isSkipped && (producedDeltas 
            || producedDroppedRequests
            || stopReason == StopReason.QUEUE_EMPTY 
            || stopReason == StopReason.NONE);
        
        if (isSuccess) {
            LOGGER.info("Delta production complete: {} deltas, {} entries, dropped request files: {}, dropped requests: {}, stop reason: {}", 
                deltaResult.getDeltasProduced(), deltaResult.getEntriesProcessed(), deltaResult.getDroppedRequestFilesProduced(), deltaResult.getDroppedRequestsProcessed(), stopReason);
            return deltaResult.toJsonWithStatus("success");
        } else {
            LOGGER.info("Delta production skipped: {}, {} entries processed, dropped request files: {}, dropped requests: {}", 
                stopReason, deltaResult.getEntriesProcessed(), deltaResult.getDroppedRequestFilesProduced(), deltaResult.getDroppedRequestsProcessed());
            return deltaResult.toJsonWithStatus("skipped", "reason", stopReason.name());
        }
    }


    /**
     * Delegates to the orchestrator to produce delta files.
     * 
     * @return DeltaProduceResult with counts and stop reason
     * @throws IOException if delta production fails
     */
    private DeltaProductionResult produceBatchedDeltas() throws IOException {
        return orchestrator.produceBatchedDeltas(this::publishDeltaProducedEvent);
    }

    private void publishDeltaProducedEvent(String deltaName) {
        vertx.eventBus().publish(this.eventDeltaProduced, deltaName);
        LOGGER.info("Published delta.produced event for: {}", deltaName);
    }
}
