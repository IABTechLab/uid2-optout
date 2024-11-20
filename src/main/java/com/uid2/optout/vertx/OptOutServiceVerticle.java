package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.auth.InternalAuthMiddleware;
import com.uid2.optout.web.QuorumWebClient;
import com.uid2.shared.Utils;
import com.uid2.shared.attest.AttestationTokenService;
import com.uid2.shared.attest.IAttestationTokenService;
import com.uid2.shared.attest.JwtService;
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
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.uid2.optout.vertx.Endpoints.*;

public class OptOutServiceVerticle extends AbstractVerticle {
    public static final String IDENTITY_HASH = "identity_hash";
    public static final String ADVERTISING_ID = "advertising_id";

    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutServiceVerticle.class);
    private final HealthComponent healthComponent = HealthManager.instance.registerComponent("http-server");
    private final AuthMiddleware auth;
    private final AttestationMiddleware attest;
    private final boolean isVerbose;
    private final int listenPort;
    private final int deltaRotateInterval;
    private final QuorumWebClient replicaWriteClient;
    private final DeliveryOptions defaultDeliveryOptions;
    private final AtomicReference<Collection<String>> cloudPaths = new AtomicReference<>();
    private final ICloudStorage cloudStorage;
    private final boolean enableOptOutPartnerMock;
    private final String internalApiKey;
    private final InternalAuthMiddleware internalAuth;

    public OptOutServiceVerticle(Vertx vertx,
                                 IAuthorizableProvider clientKeyProvider,
                                 ICloudStorage cloudStorage,
                                 JsonObject jsonConfig) {
        this.healthComponent.setHealthStatus(false, "not started");

        this.cloudStorage = cloudStorage;
        this.auth = new AuthMiddleware(clientKeyProvider);

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

        String replicaUrisConfig = jsonConfig.getString(Const.Config.OptOutReplicaUris);
        if (replicaUrisConfig == null) {
            LOGGER.warn(Const.Config.OptOutReplicaUris + " not configured, not instantiating multi-replica write client");
            this.replicaWriteClient = null;
        } else {
            String[] replicaUris = replicaUrisConfig.split(",");
            this.replicaWriteClient = new QuorumWebClient(vertx, replicaUris);
        }

        this.defaultDeliveryOptions = new DeliveryOptions();
        int addEntryTimeoutMs = jsonConfig.getInteger(Const.Config.OptOutAddEntryTimeoutMsProp);
        this.defaultDeliveryOptions.setSendTimeout(addEntryTimeoutMs);

        this.internalApiKey = jsonConfig.getString(Const.Config.OptOutInternalApiTokenProp);
        this.internalAuth = new InternalAuthMiddleware(this.internalApiKey);
        this.enableOptOutPartnerMock = jsonConfig.getBoolean(Const.Config.OptOutPartnerEndpointMockProp);
    }

    public static void sendStatus(int statusCode, HttpServerResponse response) {
        response.setStatusCode(statusCode).end();
    }

    @Override
    public void start(Promise<Void> startPromise) {
        this.healthComponent.setHealthStatus(false, "still starting");

        try {
            vertx.createHttpServer()
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

        router.route(Endpoints.OPTOUT_WRITE.toString())
                .handler(internalAuth.handle(this::handleWrite));
        router.route(Endpoints.OPTOUT_REPLICATE.toString())
                .handler(auth.handle(this::handleReplicate, Role.OPTOUT));
        router.route(Endpoints.OPTOUT_REFRESH.toString())
                .handler(auth.handle(attest.handle(this::handleRefresh, Role.OPERATOR), Role.OPERATOR));
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
        MultiMap params = req.params();
        String identityHash = req.getParam(IDENTITY_HASH);
        String advertisingId = req.getParam(ADVERTISING_ID);
        JsonObject body = routingContext.body().asJsonObject();

        HttpServerResponse resp = routingContext.response();
        if (identityHash == null || params.getAll(IDENTITY_HASH).size() != 1) {
            this.sendBadRequestError(resp);
            return;
        }
        if (advertisingId == null || params.getAll(ADVERTISING_ID).size() != 1) {
            this.sendBadRequestError(resp);
            return;
        }

        if (!this.isGetOrPost(req)) {
            this.sendBadRequestError(resp);
        } else if (body != null) {
            this.sendBadRequestError(resp);
        } else if (this.replicaWriteClient == null) {
            this.sendInternalServerError(resp, "optout replicas not configured");
        }
        else {
            try {
                this.replicaWriteClient.get(r -> {
                    r.setQueryParam(IDENTITY_HASH, identityHash);
                    r.setQueryParam(ADVERTISING_ID, advertisingId);
                    r.headers().set("Authorization", "Bearer " + internalApiKey);
                    return r;
                }).onComplete(ar -> {
                    final String maskedId1 = Utils.maskPii(identityHash);
                    final String maskedId2 = Utils.maskPii(advertisingId);
                    if (ar.failed()) {
                        LOGGER.error("failed sending optout/write to remote endpoints - identity_hash: " + maskedId1 + ", advertising_id: " + maskedId2);
                        LOGGER.error(ar.cause().getMessage(), new Exception(ar.cause()));
                        this.sendInternalServerError(resp, ar.cause().toString());
                    } else {
                        String timestamp = null;
                        for (io.vertx.ext.web.client.HttpResponse<io.vertx.core.buffer.Buffer> replicaResp : ar.result()) {
                            if (replicaResp != null && replicaResp.statusCode() == 200) {
                                timestamp = replicaResp.bodyAsString();
                            }
                        }

                        if (timestamp == null) {
                            sendInternalServerError(resp, "Unexpected result calling internal write api");
                        } else {
                            LOGGER.info("sent optout/write to remote endpoints - identity_hash: " + maskedId1 + ", advertising_id: " + maskedId1);
                            resp.setStatusCode(200)
                                    .setChunked(true)
                                    .write(timestamp);
                            resp.end();
                        }
                    }
                });
            } catch (Exception ex) {
                LOGGER.error("error creating requests for remote optout/write call:", ex);
                this.sendInternalServerError(resp, ex.getMessage());
            }
        }
    }

    private void handleWrite(RoutingContext routingContext) {
        HttpServerRequest req = routingContext.request();
        MultiMap params = req.params();
        String identityHash = req.getParam(IDENTITY_HASH);
        String advertisingId = req.getParam(ADVERTISING_ID);
        JsonObject body = routingContext.body().asJsonObject();

        HttpServerResponse resp = routingContext.response();
        if (identityHash == null || params.getAll(IDENTITY_HASH).size() != 1) {
            this.sendBadRequestError(resp);
            return;
        }
        if (advertisingId == null || params.getAll(ADVERTISING_ID).size() != 1) {
            this.sendBadRequestError(resp);
            return;
        }

        byte[] hashBytes = OptOutUtils.base64StringTobyteArray(identityHash);
        byte[] idBytes = OptOutUtils.base64StringTobyteArray(advertisingId);
        if (hashBytes == null) {
            this.sendBadRequestError(resp);
        } else if (idBytes == null) {
            this.sendBadRequestError(resp);
        } else if (!this.isGetOrPost(req)) {
            this.sendBadRequestError(resp);
        } else if (body != null) {
            this.sendBadRequestError(resp);
        } else if (OptOutEntry.isSpecialHash(hashBytes)) {
            // optout null/ones is not allowed
            this.sendBadRequestError(resp);
        } else {
            long optoutEpoch = OptOutUtils.nowEpochSeconds();
            String msg = identityHash + "," + advertisingId + "," + String.valueOf(optoutEpoch);
            vertx.eventBus().request(Const.Event.EntryAdd, msg, this.defaultDeliveryOptions,
                    ar -> this.handleEntryAdded(ar, resp, optoutEpoch));
        }
    }

    private void handleEntryAdded(AsyncResult<Message<Object>> res, HttpServerResponse resp, long optoutEpoch) {
        if (res.failed()) {
            this.sendInternalServerError(resp, res.cause().toString());
        } else if (!res.result().body().equals(true)) {
            this.sendInternalServerError(resp, "Unexpected msg reply: " + res.result().body());
        } else {
            resp.setStatusCode(200)
                    .setChunked(true)
                    .write(String.valueOf(optoutEpoch));
            resp.end();
        }
    }

    private void handleOptOutPartnerMock(RoutingContext rc) {
        this.sendOk(rc.response());
    }

    private boolean isGetOrPost(HttpServerRequest req) {
        HttpMethod method = req.method();
        return method == HttpMethod.GET || method == HttpMethod.POST;
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

    private void sendServiceUnavailableError(HttpServerResponse resp, String why) {
        if (this.isVerbose && why != null) {
            resp.setStatusCode(503);
            resp.setChunked(true);
            resp.write(why);
            resp.end();
        } else {
            sendStatus(503, resp);
        }
    }

    private void sendBadRequestError(HttpServerResponse response) {
        sendStatus(400, response);
    }

    private void sendOk(HttpServerResponse response) {
        sendStatus(200, response);
    }
}
