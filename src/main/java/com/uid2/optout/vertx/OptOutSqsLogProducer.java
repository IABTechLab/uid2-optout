package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.auth.InternalAuthMiddleware;
import com.uid2.optout.delta.DeltaFileWriter;
import com.uid2.optout.delta.ManualOverrideService;
import com.uid2.optout.delta.DeltaProductionJobStatus;
import com.uid2.optout.delta.DeltaProductionMetrics;
import com.uid2.optout.delta.DeltaProductionOrchestrator;
import com.uid2.optout.delta.DeltaProductionResult;
import com.uid2.optout.delta.S3UploadService;
import com.uid2.optout.delta.StopReason;
import com.uid2.optout.sqs.SqsWindowReader;
import com.uid2.optout.traffic.TrafficCalculator;
import com.uid2.optout.traffic.TrafficCalculator.MalformedTrafficCalcConfigException;
import com.uid2.optout.traffic.TrafficFilter;
import com.uid2.optout.traffic.TrafficFilter.MalformedTrafficFilterConfigException;
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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import static com.uid2.optout.util.HttpResponseHelper.*;

import java.net.URI;

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
 * <p><strong>Note:</strong> The delta construction and circuit breaking is delegated to DeltaProductionOrchestrator.</p>
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
    private final TrafficFilter trafficFilter;
    private final TrafficCalculator trafficCalculator;
    private final DeltaProductionOrchestrator orchestrator;
    
    /*
     * Tracks the current delta production job status for this pod.
     * Used to prevent concurrent jobs from running.
     */
    private final AtomicReference<DeltaProductionJobStatus> currentJob = new AtomicReference<>(null);
    
    private volatile boolean shutdownInProgress = false;

    public OptOutSqsLogProducer(JsonObject jsonConfig, ICloudStorage cloudStorage, ICloudStorage cloudStorageDroppedRequests, OptOutCloudSync cloudSync, String eventDeltaProduced, SqsClient sqsClient) throws IOException, MalformedTrafficCalcConfigException, MalformedTrafficFilterConfigException {
        this.eventDeltaProduced = eventDeltaProduced;
        
        // initialize sqs client
        String queueUrl = jsonConfig.getString(Const.Config.OptOutSqsQueueUrlProp);
        if (queueUrl == null || queueUrl.isEmpty()) {
            throw new IOException("sqs queue url not configured");
        }
        if (sqsClient != null) {
            this.sqsClient = sqsClient;
        } else {
            SqsClientBuilder builder = SqsClient.builder();
            // Support custom endpoint for LocalStack
            String awsEndpoint = jsonConfig.getString(Const.Config.AwsSqsEndpointProp);
            if (awsEndpoint != null && !awsEndpoint.isEmpty()) {
                builder.endpointOverride(URI.create(awsEndpoint));
                String region = jsonConfig.getString(Const.Config.AwsRegionProp);
                if (region == null || region.isEmpty()) {
                    throw new IllegalArgumentException("aws_region must be configured when using custom SQS endpoint");
                }
                builder.region(Region.of(region));
                // LocalStack requires credentials (any value works)
                builder.credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")));
                LOGGER.info("SQS client using custom endpoint: {}, region: {}", awsEndpoint, region);
            }
            this.sqsClient = builder.build();
        }
        LOGGER.info("sqs client initialized for queue: {}", queueUrl);

        // http server configuration
        this.listenPort = Const.Port.ServicePortForOptOut + Utils.getPortOffset() + 1;
        
        // authentication
        String internalApiKey = jsonConfig.getString(Const.Config.OptOutInternalApiTokenProp);
        this.internalAuth = new InternalAuthMiddleware(internalApiKey, "optout-sqs");

        // circuit breaker tools
        this.trafficFilter = new TrafficFilter(jsonConfig.getString(Const.Config.TrafficFilterConfigPathProp));
        this.trafficCalculator = new TrafficCalculator(cloudStorage, jsonConfig.getString(Const.Config.OptOutS3FolderProp), jsonConfig.getString(Const.Config.TrafficCalcConfigPathProp));

        // configuration values for orchestrator setup
        int replicaId = OptOutUtils.getReplicaId(jsonConfig); 
        int maxMessagesPerPoll = 10; // sqs max is 10
        int deltaWindowSeconds = jsonConfig.getInteger(Const.Config.OptOutSqsDeltaWindowSecondsProp, 300); // fixed 5 minutes, allow config for testing
        int visibilityTimeout = jsonConfig.getInteger(Const.Config.OptOutSqsVisibilityTimeoutProp, 240);
        int jobTimeoutSeconds = jsonConfig.getInteger(Const.Config.OptOutDeltaJobTimeoutSecondsProp, 10800);
        int maxMessagesPerFile = jsonConfig.getInteger(Const.Config.OptOutMaxMessagesPerFileProp, 10000);
        int bufferSize = jsonConfig.getInteger(Const.Config.OptOutProducerBufferSizeProp);

        // orchestrator setup
        DeltaFileWriter deltaFileWriter = new DeltaFileWriter(bufferSize);
        S3UploadService deltaUploadService = new S3UploadService(cloudStorage, this.sqsClient, queueUrl);
        S3UploadService droppedRequestUploadService = new S3UploadService(cloudStorageDroppedRequests, this.sqsClient, queueUrl) ;
        S3UploadService malformedRequestUploadService = new S3UploadService(cloudStorageDroppedRequests, this.sqsClient, queueUrl);
        ManualOverrideService manualOverrideService = new ManualOverrideService(cloudStorage, jsonConfig.getString(Const.Config.ManualOverrideS3PathProp));
        SqsWindowReader windowReader = new SqsWindowReader(
            this.sqsClient, queueUrl, maxMessagesPerPoll, 
            visibilityTimeout, deltaWindowSeconds, maxMessagesPerFile,
            malformedRequestUploadService, "malformed/", replicaId
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
        
        LOGGER.info("initialized with maxMessagesPerFile={}, maxMessagesPerPoll={}, visibilityTimeout={}, deltaWindowSeconds={}, jobTimeoutSeconds={}",
            maxMessagesPerFile, maxMessagesPerPoll, visibilityTimeout, deltaWindowSeconds, jobTimeoutSeconds);
    }

    @Override
    public void start(Promise<Void> startPromise) {
        LOGGER.info("starting http server on port {}", listenPort);

        try {
            vertx.createHttpServer()
                    .requestHandler(createRouter())
                    .listen(listenPort, result -> {
                        if (result.succeeded()) {
                            this.healthComponent.setHealthStatus(true);
                            LOGGER.info("http server started on port {}", listenPort);
                            startPromise.complete();
                        } else {
                            LOGGER.error("failed to start http server", result.cause());
                            this.healthComponent.setHealthStatus(false, result.cause().getMessage());
                            startPromise.fail(result.cause());
                        }
                    });

        } catch (Exception e) {
            LOGGER.error("failed to start http server", e);
            this.healthComponent.setHealthStatus(false, e.getMessage());
            startPromise.fail(e);
        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        LOGGER.info("stopping");
        this.shutdownInProgress = true;

        if (this.sqsClient != null) {
            try {
                this.sqsClient.close();
            } catch (Exception e) {
                LOGGER.error("error closing sqs client", e);
            }
        }

        stopPromise.complete();
        LOGGER.info("stopped");
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
            sendIdle(resp, "no job running on this pod");
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

        LOGGER.info("delta production job requested");

        try {
            this.trafficFilter.reloadTrafficFilterConfig();
        } catch (MalformedTrafficFilterConfigException e) {
            LOGGER.error("circuit_breaker_config_error: failed to reload traffic filter config: {}", e.getMessage(), e);
            sendError(resp, e);
            return;
        }

        try {
            this.trafficCalculator.reloadTrafficCalcConfig();
        } catch (MalformedTrafficCalcConfigException e) {
            LOGGER.error("circuit_breaker_config_error: failed to reload traffic calc config: {}", e.getMessage(), e);
            sendError(resp, e);
            return;
        }

        DeltaProductionJobStatus existingJob = currentJob.get();
        
        // if there's an existing job, check if it's still running
        if (existingJob != null) {
            if (existingJob.getState() == DeltaProductionJobStatus.JobState.RUNNING) {
                LOGGER.info("job already running, returning conflict");
                sendConflict(resp, "job already running on this pod");
                return;
            }
            
            LOGGER.info("clearing previous {} job", existingJob.getState());
        }

        DeltaProductionJobStatus newJob = new DeltaProductionJobStatus();

        // try to set the new job
        if (!currentJob.compareAndSet(existingJob, newJob)) {
            sendConflict(resp, "job state changed, please retry");
            return;
        }

        LOGGER.info("starting new job");
        this.startDeltaProductionJob(newJob);

        // return immediately with 202 Accepted
        sendAccepted(resp, "job started");
    }

    /**
     * Starts the delta production job asynchronously
     * The job runs on a worker thread and updates the DeltaProduceJobStatus when complete
     */
    private void startDeltaProductionJob(DeltaProductionJobStatus job) {
        vertx.executeBlocking(() -> produceDeltasBlocking()).onComplete(ar -> {
            if (ar.succeeded()) {
                job.complete(ar.result());
            } else {
                job.fail(ar.cause().getMessage());
                LOGGER.error("delta_job_failed: {}", ar.cause().getMessage(), ar.cause());
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
            throw new Exception("producer is shutting down");
        }

        DeltaProductionResult result = orchestrator.produceBatchedDeltas(this::publishDeltaProducedEvent);

        StopReason stopReason = result.getStopReason();
        boolean producedWork = result.getDeltasProduced() > 0 || result.getDroppedRequestFilesProduced() > 0;
        boolean halted = stopReason == StopReason.CIRCUIT_BREAKER_TRIGGERED || stopReason == StopReason.MANUAL_OVERRIDE_ACTIVE;

        String status = halted ? "halted" : (producedWork ? "success" : "skipped");
        
        LOGGER.info("delta production {}: {} deltas, {} entries, {} dropped files, {} dropped requests, reason={}",
                status, result.getDeltasProduced(), result.getEntriesProcessed(), 
                result.getDroppedRequestFilesProduced(), result.getDroppedRequestsProcessed(), stopReason);

        return result.toJsonWithStatus(status);
    }

    private void publishDeltaProducedEvent(String deltaName) {
        vertx.eventBus().publish(this.eventDeltaProduced, deltaName);
    }
}
