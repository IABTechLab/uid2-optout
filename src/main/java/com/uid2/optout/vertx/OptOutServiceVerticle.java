package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.sqs.SqsMessageOperations;
import com.uid2.shared.Utils;
import com.uid2.shared.attest.AttestationTokenService;
import com.uid2.shared.attest.IAttestationTokenService;
import com.uid2.shared.attest.JwtService;
import com.uid2.shared.audit.Audit;
import com.uid2.shared.auth.IAuthorizableProvider;
import com.uid2.shared.auth.OperatorKey;
import com.uid2.shared.auth.Role;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.health.HealthComponent;
import com.uid2.shared.health.HealthManager;
import com.uid2.shared.middleware.AttestationMiddleware;
import com.uid2.shared.middleware.AuthMiddleware;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.optout.OptOutFileMetadata;
import com.uid2.shared.optout.OptOutMetadata;
import com.uid2.shared.optout.OptOutUtils;
import com.uid2.shared.vertx.RequestCapturingHandler;
import io.vertx.core.*;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class OptOutServiceVerticle extends AbstractVerticle {
    public static final String IDENTITY_HASH = "identity_hash";
    public static final String ADVERTISING_ID = "advertising_id";
    public static final String UID_TRACE_ID = "UID-Trace-Id";
    public static final String CLIENT_IP = "client_ip";
    public static final String EMAIL = "email";
    public static final String PHONE = "phone";
    public static final long MAX_REQUEST_BODY_SIZE = 1 << 20; // 1MB

    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutServiceVerticle.class);
    private final HealthComponent healthComponent = HealthManager.instance.registerComponent("http-server");
    private final AuthMiddleware auth;
    private final AttestationMiddleware attest;
    private final boolean isVerbose;
    private final int listenPort;
    private final int deltaRotateInterval;
    private final AtomicReference<Collection<String>> cloudPaths = new AtomicReference<>();
    private final ICloudStorage cloudStorage;
    private final boolean enableOptOutPartnerMock;
    private final SqsClient sqsClient;
    private final String sqsQueueUrl;
    private final int sqsMaxQueueSize;

    public OptOutServiceVerticle(Vertx vertx,
                                 IAuthorizableProvider clientKeyProvider,
                                 ICloudStorage cloudStorage,
                                 JsonObject jsonConfig) {
        this.healthComponent.setHealthStatus(false, "not started");

        this.cloudStorage = cloudStorage;
        this.auth = new AuthMiddleware(clientKeyProvider, "optout");

        final String attestEncKey = jsonConfig.getString(Const.Config.AttestationEncryptionKeyName);
        final String attestEncSalt = jsonConfig.getString(Const.Config.AttestationEncryptionSaltName);
        final String jwtAudience = jsonConfig.getString(Const.Config.OptOutUrlProp);
        final String jwtIssuer = jsonConfig.getString(Const.Config.CorePublicUrlProp);
        Boolean enforceJwt = jsonConfig.getBoolean(Const.Config.EnforceJwtProp, true);
        if (enforceJwt == null) {
            enforceJwt = true;
        }

        final JwtService jwtService = new JwtService(jsonConfig);

        final IAttestationTokenService tokenService = new AttestationTokenService(attestEncKey, attestEncSalt);
        this.attest = new AttestationMiddleware(tokenService, jwtService, jwtAudience, jwtIssuer, enforceJwt);

        this.listenPort = Const.Port.ServicePortForOptOut + Utils.getPortOffset();
        this.deltaRotateInterval = jsonConfig.getInteger(Const.Config.OptOutDeltaRotateIntervalProp);
        this.isVerbose = jsonConfig.getBoolean(Const.Config.ServiceVerboseProp, false);

        this.enableOptOutPartnerMock = jsonConfig.getBoolean(Const.Config.OptOutPartnerEndpointMockProp);

        this.sqsQueueUrl = jsonConfig.getString(Const.Config.OptOutSqsQueueUrlProp);
        this.sqsMaxQueueSize = jsonConfig.getInteger(Const.Config.OptOutSqsMaxQueueSizeProp, 0); // 0 = no limit

        SqsClient tempSqsClient = null;
        if (this.sqsQueueUrl == null || this.sqsQueueUrl.isEmpty()) {
            LOGGER.error("sqs_error: queue url not configured");
        } else {
            try {
                SqsClientBuilder builder = SqsClient.builder();
                
                // Support custom endpoint for LocalStack
                String awsEndpoint = jsonConfig.getString(Const.Config.AwsSqsEndpointProp);
                LOGGER.info("SQS endpoint from config: {}", awsEndpoint);
                if (awsEndpoint != null && !awsEndpoint.isEmpty()) {
                    builder.endpointOverride(URI.create(awsEndpoint));
                    // Use raw string "aws_region" to ensure correct config key
                    String region = jsonConfig.getString("aws_region");
                    LOGGER.info("AWS region from config: {}", region);
                    if (region == null || region.isEmpty()) {
                        throw new IllegalArgumentException("aws_region must be configured when using custom SQS endpoint");
                    }
                    builder.region(Region.of(region));
                    // LocalStack requires credentials (any value works)
                    builder.credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("test", "test")));
                    LOGGER.info("SQS client using custom endpoint: {}, region: {}", awsEndpoint, region);
                }
                
                tempSqsClient = builder.build();
                LOGGER.info("SQS client initialized successfully");
                LOGGER.info("SQS client region: " + tempSqsClient.serviceClientConfiguration().region());
                LOGGER.info("SQS queue URL configured: " + this.sqsQueueUrl);
            } catch (Exception e) {
                LOGGER.error("Failed to initialize SQS client: " + e.getMessage(), e);
                tempSqsClient = null;
            }
        }
        this.sqsClient = tempSqsClient;
    }

    public static void sendStatus(int statusCode, HttpServerResponse response) {
        response.setStatusCode(statusCode).end();
    }

    @Override
    public void start(Promise<Void> startPromise) {
        this.healthComponent.setHealthStatus(false, "still starting");

        try {
            vertx.createHttpServer(new HttpServerOptions().setMaxFormBufferedBytes((int) MAX_REQUEST_BODY_SIZE))
                    .requestHandler(createRouter())
                    .listen(listenPort, result -> handleListenResult(startPromise, result));
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            startPromise.fail(new Throwable(ex));
        }

        startPromise.future()
                .onSuccess(v -> LOGGER.info("OptOutServiceVerticle started on HTTP port: {}", listenPort))
                .onFailure(e -> {
                    LOGGER.error("OptOutServiceVerticle failed to start", e);
                    this.healthComponent.setHealthStatus(false, e.getMessage());
                });
    }

    @Override
    public void stop() {
        LOGGER.info("Shutting down OptOutServiceVerticle");
        if (this.sqsClient != null) {
            try {
                this.sqsClient.close();
                LOGGER.info("SQS client closed");
            } catch (Exception e) {
                LOGGER.error("Error closing SQS client", e);
            }
        }
    }

    public void setCloudPaths(Collection<String> paths) {
        // service is not healthy until it received list of paths
        this.healthComponent.setHealthStatus(true);
        this.cloudPaths.set(paths);
    }

    private void handleListenResult(Promise<Void> startPromise, AsyncResult<HttpServer> result) {
        if (result.succeeded()) {
            startPromise.complete();
        } else {
            LOGGER.error("listen failed: " + result.cause());
            startPromise.fail(new Throwable(result.cause()));
        }
    }

    private Router createRouter() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route().handler(new RequestCapturingHandler());
        router.route().handler(CorsHandler.create()
                .addRelativeOrigin(".*.")
                .allowedMethod(io.vertx.core.http.HttpMethod.GET)
                .allowedMethod(io.vertx.core.http.HttpMethod.POST)
                .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
                .allowedHeader("Access-Control-Request-Method")
                .allowedHeader("Access-Control-Allow-Credentials")
                .allowedHeader("Access-Control-Allow-Origin")
                .allowedHeader("Access-Control-Allow-Headers")
                .allowedHeader("Content-Type"));

        router.route(Endpoints.OPTOUT_REPLICATE.toString())
                .handler(auth.handleWithAudit(this::handleReplicate, Arrays.asList(Role.OPTOUT)));
        router.route(Endpoints.OPTOUT_REFRESH.toString())
                .handler(auth.handleWithAudit(attest.handle(this::handleRefresh, Role.OPERATOR), Arrays.asList(Role.OPERATOR)));
        router.get(Endpoints.OPS_HEALTHCHECK.toString())
                .handler(this::handleHealthCheck);

        if (this.enableOptOutPartnerMock) {
            final OperatorKey loopbackClient = new OperatorKey("", "", "loopback", "loopback", "loopback", 0, false, "");
            router.route(Endpoints.OPTOUT_PARTNER_MOCK.toString()).handler(auth.loopbackOnly(this::handleOptOutPartnerMock, loopbackClient));
        }

        //// if enabled, this would add handler for exposing prometheus metrics
        // router.route("/metrics").handler(PrometheusScrapingHandler.create());

        return router;
    }

    private void handleRefresh(RoutingContext routingContext) {
        HttpServerResponse resp = routingContext.response();
        Collection<String> pathsToSign = this.cloudPaths.get();
        if (pathsToSign == null) {
            sendStatus(404, routingContext.response());
            return;
        }

        // remove files not relevant for optout client
        Instant lastSnap = OptOutUtils.lastPartitionTimestamp(pathsToSign)
                .minusSeconds(this.deltaRotateInterval * 3);
        pathsToSign = pathsToSign.stream()
                .filter(f -> OptOutUtils.isPartitionFile(f) || !OptOutUtils.isDeltaBeforePartition(lastSnap, f))
                .collect(Collectors.toList());

        try {
            OptOutMetadata metadata = new OptOutMetadata();

            String lastFilePath = pathsToSign.stream()
                    .sorted(OptOutUtils.DeltaFilenameComparator)
                    .reduce((a, b) -> b).orElse(null);

            metadata.version = lastFilePath == null ?
                    Instant.now().getEpochSecond() : OptOutUtils.getFileEpochSeconds(lastFilePath);

            metadata.generated = metadata.version;
            metadata.optoutLogs = new ArrayList<>();
            for (String pathToSign : pathsToSign) {
                URL signedUrl = this.cloudStorage.preSignUrl(pathToSign);
                OptOutFileMetadata mdFile = new OptOutFileMetadata();
                mdFile.from = OptOutUtils.getFileEpochSeconds(pathToSign);
                mdFile.to = mdFile.from + this.deltaRotateInterval;
                mdFile.type = OptOutUtils.isDeltaFile(pathToSign) ? "delta" : "partition";
                mdFile.location = signedUrl.toString();
                metadata.optoutLogs.add(mdFile);
            }

            resp.putHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            resp.end(metadata.toJsonString());
        } catch (Exception ex) {
            this.sendInternalServerError(resp, ex.getMessage());
        }
    }

    private void handleHealthCheck(RoutingContext rc) {
        if (HealthManager.instance.isHealthy()) {
            this.sendOk(rc.response());
        } else {
            HttpServerResponse resp = rc.response();
            String reason = HealthManager.instance.reason();
            resp.setStatusCode(503);
            resp.setChunked(true);
            resp.write(reason);
            resp.end();
        }
    }

    private void handleReplicate(RoutingContext routingContext) {
        HttpServerRequest req = routingContext.request();
        HttpServerResponse resp = routingContext.response();

        // skip sqs queueing for validator operators (reference-operator, candidate-operator)
        // this avoids triple processing of the same request
        String instanceId = req.getHeader(Audit.UID_INSTANCE_ID_HEADER);
        if (isValidatorOperatorRequest(instanceId)) {
            long optoutEpoch = OptOutUtils.nowEpochSeconds();
            resp.setStatusCode(200)
                    .setChunked(true)
                    .write(String.valueOf(optoutEpoch));
            resp.end();
            return;
        }

        MultiMap params = req.params();
        String identityHash = req.getParam(IDENTITY_HASH);
        String advertisingId = req.getParam(ADVERTISING_ID);
        JsonObject body = routingContext.body().asJsonObject();
        String traceId = req.getHeader(UID_TRACE_ID);
        String clientIp = body != null ? body.getString(CLIENT_IP) : null;
        String email = body != null ? body.getString(EMAIL) : null;
        String phone = body != null ? body.getString(PHONE) : null;

        // validate parameters
        if (identityHash == null || params.getAll(IDENTITY_HASH).size() != 1) {
            this.sendBadRequestError(resp);
            return;
        }
        if (advertisingId == null || params.getAll(ADVERTISING_ID).size() != 1) {
            this.sendBadRequestError(resp);
            return;
        }

        // validate base64 decoding
        byte[] hashBytes = OptOutUtils.base64StringTobyteArray(identityHash);
        byte[] idBytes = OptOutUtils.base64StringTobyteArray(advertisingId);
        if (hashBytes == null) {
            this.sendBadRequestError(resp);
            return;
        }
        if (idBytes == null) {
            this.sendBadRequestError(resp);
            return;
        }

        // optout null/ones is not allowed
        if (OptOutEntry.isSpecialHash(hashBytes)) {
            this.sendBadRequestError(resp);
            return;
        }

        if (!this.isGetOrPost(req)) {
            this.sendBadRequestError(resp);
            return;
        }

        if (this.sqsClient == null) {
            this.sendInternalServerError(resp, "SQS client not initialized");
            return;
        }

        try {
            JsonObject messageBody = new JsonObject()
                    .put(IDENTITY_HASH, identityHash)
                    .put(ADVERTISING_ID, advertisingId)
                    .put("uid_trace_id", traceId)
                    .put("client_ip", clientIp)
                    .put("email", email)
                    .put("phone", phone);

            // send message to sqs queue
            vertx.executeBlocking(() -> {
                // check queue size limit before sending
                if (this.sqsMaxQueueSize > 0) {
                    SqsMessageOperations.QueueAttributes queueAttrs = 
                        SqsMessageOperations.getQueueAttributes(this.sqsClient, this.sqsQueueUrl);
                    if (queueAttrs != null) {
                        int currentSize = queueAttrs.getTotalMessages();
                        if (currentSize >= this.sqsMaxQueueSize) {
                            LOGGER.warn("sqs_queue_full: rejecting message, currentSize={}, maxSize={}", 
                                currentSize, this.sqsMaxQueueSize);
                            throw new IllegalStateException("queue size limit exceeded");
                        }
                    }
                }

                SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                        .queueUrl(this.sqsQueueUrl)
                        .messageBody(messageBody.encode())
                        .build();

                this.sqsClient.sendMessage(sendMsgRequest);
                return OptOutUtils.nowEpochSeconds();
            }).onSuccess(optoutEpoch -> {
                resp.setStatusCode(200)
                        .setChunked(true)
                        .write(String.valueOf(optoutEpoch));
                resp.end();
            }).onFailure(cause -> {
                LOGGER.error("failed to queue message, cause={}", cause.getMessage());
                this.sendInternalServerError(resp, cause.getMessage());
            });
        } catch (Exception ex) {
            LOGGER.error("Error processing replicate request: " + ex.getMessage(), ex);
            this.sendInternalServerError(resp, ex.getMessage());
        }
    }

    private void handleOptOutPartnerMock(RoutingContext rc) {
        this.sendOk(rc.response());
    }

    private boolean isGetOrPost(HttpServerRequest req) {
        HttpMethod method = req.method();
        return method == HttpMethod.GET || method == HttpMethod.POST;
    }

    private boolean isValidatorOperatorRequest(String instanceId) {
        if (instanceId == null) {
            return false;
        }
        return instanceId.contains("reference-operator") || instanceId.contains("candidate-operator");
    }

    private void sendInternalServerError(HttpServerResponse resp, String why) {
        if (this.isVerbose && why != null) {
            resp.setStatusCode(500);
            resp.setChunked(true);
            resp.write(why);
            resp.end();
        } else {
            sendStatus(500, resp);
        }
    }

    private void sendBadRequestError(HttpServerResponse response) {
        sendStatus(400, response);
    }

    private void sendOk(HttpServerResponse response) {
        sendStatus(200, response);
    }
}
