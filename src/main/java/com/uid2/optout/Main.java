package com.uid2.optout;

import com.uid2.optout.vertx.*;
import com.uid2.optout.traffic.TrafficFilter.MalformedTrafficFilterConfigException;
import com.uid2.optout.traffic.TrafficCalculator.MalformedTrafficCalcConfigException;
import com.uid2.shared.ApplicationVersion;
import com.uid2.shared.Utils;
import com.uid2.shared.attest.AttestationResponseHandler;
import com.uid2.shared.attest.NoAttestationProvider;
import com.uid2.shared.attest.UidCoreClient;
import com.uid2.shared.audit.UidInstanceIdProvider;
import com.uid2.shared.auth.RotatingOperatorKeyProvider;
import com.uid2.shared.cloud.*;
import com.uid2.shared.health.HealthManager;
import com.uid2.shared.jmx.AdminApi;
import com.uid2.shared.optout.OptOutCloudSync;
import com.uid2.shared.optout.OptOutUtils;
import com.uid2.shared.store.CloudPath;
import com.uid2.shared.store.scope.GlobalScope;
import com.uid2.shared.vertx.CloudSyncVerticle;
import com.uid2.shared.vertx.RotatingStoreVerticle;
import com.uid2.shared.vertx.VertxUtils;
import com.uid2.shared.util.HTTPPathMetricFilter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusRenameFilter;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.*;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MetricsDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.micrometer.Label;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

//
// produces events:
//   - cloudsync.optout.refresh (timer-based)
//   - delta.produce            (timer-based)
//
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private final Vertx vertx;
    private final JsonObject config;
    private final ICloudStorage fsLocal = new LocalStorageMock();
    private final ICloudStorage fsOptOut;
    private final DownloadCloudStorage fsOperatorKeyConfig;
    private final ICloudStorage fsPartnerConfig;
    private final RotatingOperatorKeyProvider operatorKeyProvider;
    private final boolean observeOnly;
    private final boolean enqueueSqsEnabled;
    private final UidInstanceIdProvider uidInstanceIdProvider;

    public Main(Vertx vertx, JsonObject config) throws Exception {
        this.vertx = vertx;
        this.config = config;
        this.observeOnly = config.getBoolean(Const.Config.OptOutObserveOnlyProp);
        if (this.observeOnly) {
            LOGGER.warn("Running Observe ONLY mode: no producer, no sender");
        }
        this.enqueueSqsEnabled = config.getBoolean(Const.Config.OptOutSqsEnabledProp, false);
        this.uidInstanceIdProvider = new UidInstanceIdProvider(config);

        boolean useStorageMock = config.getBoolean(Const.Config.StorageMockProp, false);
        if (useStorageMock) {
            Path cloudMockPath = Paths.get(config.getString(Const.Config.OptOutDataDirProp), "cloud_mock");
            Utils.ensureDirectoryExists(cloudMockPath);
            this.fsOptOut = new LocalStorageMock(cloudMockPath.toString());
            LOGGER.info("Using LocalStorageMock for optout: " + cloudMockPath.toString());

            this.fsPartnerConfig = new EmbeddedResourceStorage(Main.class);
            LOGGER.info("Partners config - Using EmbeddedResourceStorage");
        } else {
            String optoutBucket = this.config.getString(Const.Config.OptOutS3BucketProp);
            ICloudStorage cs = CloudUtils.createStorage(optoutBucket, config);
            if (config.getBoolean(Const.Config.OptOutS3PathCompatProp)) {
                LOGGER.warn("Using S3 Path Compatibility Conversion: log -> delta, snapshot -> partition");
                this.fsOptOut = new PathConversionWrapper(
                    cs,
                    in -> {
                        String out = in.replace("log", "delta")
                            .replace("snapshot", "partition");
                        LOGGER.trace("S3 path forward convert: " + in + " -> " + out);
                        return out;
                    },
                    in -> {
                        String out = in.replace("delta", "log")
                            .replace("partition", "snapshot");
                        LOGGER.trace("S3 path backward convert: " + in + " -> " + out);
                        return out;
                    }
                );
            } else {
                this.fsOptOut = cs;
            }
            LOGGER.info("Using CloudStorage for optout: s3://" + optoutBucket);

            this.fsPartnerConfig = CloudUtils.createStorage(optoutBucket, config);
            LOGGER.info("Using CloudStorage for partners config: s3://" + optoutBucket);
        }

        ApplicationVersion appVersion = ApplicationVersion.load("uid2-optout", "uid2-shared", "uid2-attestation-api");

        String coreAttestUrl = this.config.getString(Const.Config.CoreAttestUrlProp);
        final DownloadCloudStorage contentStorage;
        if (coreAttestUrl != null) {
            String coreApiToken = this.config.getString(Const.Config.CoreApiTokenProp);
            if (coreApiToken == null || coreApiToken.isBlank()) {
                LOGGER.warn("Core Attest URL set, but the API Token is not set");
            }
            var tokenRetriever = new AttestationResponseHandler(vertx, coreAttestUrl, coreApiToken, "public", appVersion, new NoAttestationProvider(), null, CloudUtils.defaultProxy, this.uidInstanceIdProvider);
            UidCoreClient uidCoreClient = UidCoreClient.createNoAttest(coreApiToken, tokenRetriever, this.uidInstanceIdProvider);
            if (useStorageMock) uidCoreClient.setAllowContentFromLocalFileSystem(true);
            this.fsOperatorKeyConfig = uidCoreClient;
            contentStorage = uidCoreClient.getContentStorage();
            LOGGER.info("Operator api-keys - Using uid2-core attestation endpoint: " + coreAttestUrl);
        } else if (useStorageMock) {
            this.fsOperatorKeyConfig = new EmbeddedResourceStorage(Main.class);
            contentStorage = this.fsOperatorKeyConfig;
            LOGGER.info("Client api-keys - Using EmbeddedResourceStorage");
        } else {
            String optoutS3Bucket = this.config.getString(Const.Config.OptOutS3BucketProp);
            this.fsOperatorKeyConfig = CloudUtils.createStorage(optoutS3Bucket, config);
            contentStorage = this.fsOperatorKeyConfig;
            LOGGER.info("Using CloudStorage for operator api-key at s3://" + optoutS3Bucket);
        }

        String operatorsMdPath = this.config.getString(Const.Config.OperatorsMetadataPathProp);
        this.operatorKeyProvider = new RotatingOperatorKeyProvider(
                this.fsOperatorKeyConfig,
                contentStorage,
                new GlobalScope(new CloudPath(operatorsMdPath)));
        if (useStorageMock) {
            this.operatorKeyProvider.loadContent(this.operatorKeyProvider.getMetadata());
        }
    }

    public static void main(String[] args) {
        final String vertxConfigPath = System.getProperty(Const.Config.VERTX_CONFIG_PATH_PROP);
        if (vertxConfigPath != null) {
            LOGGER.info("Running CUSTOM CONFIG mode, config: {}", vertxConfigPath);
        }
        else if (!Utils.isProductionEnvironment()) {
            LOGGER.info("Running LOCAL DEBUG mode, config: {}", Const.Config.LOCAL_CONFIG_PATH);
            System.setProperty(Const.Config.VERTX_CONFIG_PATH_PROP, Const.Config.LOCAL_CONFIG_PATH);
        } else {
            LOGGER.info("Running PRODUCTION mode, config: {}", Const.Config.OVERRIDE_CONFIG_PATH);
        }

        // create AdminApi instance
        try {
            ObjectName objectName = new ObjectName("uid2.optout:type=jmx,name=AdminApi");
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            server.registerMBean(AdminApi.instance, objectName);
        } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | MalformedObjectNameException e) {
            LOGGER.error(e.getMessage(), e);
            System.exit(-1);
        }

        final int portOffset = Utils.getPortOffset();
        VertxPrometheusOptions prometheusOptions = new VertxPrometheusOptions()
            .setStartEmbeddedServer(true)
            .setEmbeddedServerOptions(new HttpServerOptions().setPort(Const.Port.PrometheusPortForOptOut + portOffset))
            .setEnabled(true);

        MicrometerMetricsOptions metricOptions = new MicrometerMetricsOptions()
            .setPrometheusOptions(prometheusOptions)
            .setLabels(EnumSet.of(Label.HTTP_METHOD, Label.HTTP_CODE, Label.HTTP_PATH))
            .setJvmMetricsEnabled(true)
            .setEnabled(true);
        setupMetrics(metricOptions);

        final int threadBlockedCheckInterval = Utils.isProductionEnvironment()
            ? 60 * 1000
            : 3600 * 1000;

        VertxOptions vertxOptions = new VertxOptions()
            .setMetricsOptions(metricOptions)
            .setBlockedThreadCheckInterval(threadBlockedCheckInterval);

        Vertx vertx = Vertx.vertx(vertxOptions);

        ConfigRetriever retriever = VertxUtils.createConfigRetriever(vertx);
        retriever.getConfig(ar -> {
            if (ar.failed()) {
                LOGGER.error("Unable to read config: " + ar.cause().getMessage(), ar.cause());
                return;
            }
            try {
                Main app = new Main(vertx, ar.result());
                app.run(args);
            } catch (Exception e) {
                LOGGER.error("Unable to create/run application: " + e.getMessage(), e);
                vertx.close();
                System.exit(1);
            }
        });
    }

    private static void setupMetrics(MicrometerMetricsOptions metricOptions) {
        BackendRegistries.setupBackend(metricOptions, null);

        // As of now default backend registry should have been created
        if (BackendRegistries.getDefaultNow() instanceof PrometheusMeterRegistry) {
            PrometheusMeterRegistry prometheusRegistry = (PrometheusMeterRegistry) BackendRegistries.getDefaultNow();

            // see also https://micrometer.io/docs/registry/prometheus
            prometheusRegistry.config()
                // providing common renaming for prometheus metric, e.g. "hello.world" to "hello_world"
                .meterFilter(new PrometheusRenameFilter())
                    .meterFilter(MeterFilter.replaceTagValues(Label.HTTP_PATH.toString(),
                            actualPath -> HTTPPathMetricFilter.filterPath(actualPath, Endpoints.pathSet())))
                // Don't record metrics for 404s.
                .meterFilter(MeterFilter.deny(id ->
                    id.getName().startsWith(MetricsDomain.HTTP_SERVER.getPrefix()) &&
                    Objects.equals(id.getTag(Label.HTTP_CODE.toString()), "404")))
                // adding common labels
                .commonTags("application", "uid2-optout");

            // wire my monitoring system to global static state, see also https://micrometer.io/docs/concepts
            Metrics.addRegistry(prometheusRegistry);
        }
    }

    public void run(String[] args) throws IOException {
        this.createAppStatusMetric();
        this.createVertxInstancesMetric();
        this.createVertxEventLoopsMetric();

        List<Future> futs = new ArrayList<>();

        // create optout cloud sync verticle
        OptOutCloudSync cs = new OptOutCloudSync(this.config, true);
        CloudSyncVerticle cloudSyncVerticle = new CloudSyncVerticle("optout", this.fsOptOut, this.fsLocal, cs, this.config);

        // deploy optout cloud sync verticle
        futs.add(this.deploySingleInstance(cloudSyncVerticle));

        // deploy operator key rotator
        futs.add(this.createOperatorKeyRotator());

        if (!this.observeOnly) {
            // enable partition producing
            cs.enableDeltaMerging(vertx, Const.Event.PartitionProduce);

            // create partners config monitor
            futs.add(this.createPartnerConfigMonitor(cloudSyncVerticle.eventDownloaded()));

            // create & deploy log producer verticle
            String eventUpload = cloudSyncVerticle.eventUpload();
            OptOutLogProducer logProducer = new OptOutLogProducer(this.config, eventUpload, eventUpload);
            futs.add(this.deploySingleInstance(logProducer));

            // upload last delta produced and potentially not uploaded yet
            futs.add((this.uploadLastDelta(cs, logProducer, cloudSyncVerticle.eventUpload(), cloudSyncVerticle.eventRefresh())));
        }

        // deploy sqs producer if enabled
        if (this.enqueueSqsEnabled) {
            LOGGER.info("SQS enabled, deploying OptOutSqsLogProducer");
            try {
                // create sqs-specific cloud sync with custom folder (default: "sqs-delta")
                String sqsFolder = this.config.getString(Const.Config.OptOutSqsS3FolderProp, "sqs-delta");
                LOGGER.info("sqs config - optout_sqs_s3_folder: {}, will override optout_s3_folder to: {}", 
                    sqsFolder, sqsFolder);
                JsonObject sqsConfig = new JsonObject().mergeIn(this.config)
                    .put(Const.Config.OptOutS3FolderProp, sqsFolder);
                LOGGER.info("sqs config after merge - optout_s3_folder: {}", sqsConfig.getString(Const.Config.OptOutS3FolderProp));
                OptOutCloudSync sqsCs = new OptOutCloudSync(sqsConfig, true);

                // create sqs-specific cloud storage instance (same bucket, different folder handling)
                ICloudStorage fsSqs;
                boolean useStorageMock = this.config.getBoolean(Const.Config.StorageMockProp, false);
                if (useStorageMock) {
                    // reuse the same LocalStorageMock for testing
                    fsSqs = this.fsOptOut;
                } else {
                    // create fresh CloudStorage for SQS (no path conversion wrapper)
                    String optoutBucket = this.config.getString(Const.Config.OptOutS3BucketProp);
                    fsSqs = CloudUtils.createStorage(optoutBucket, sqsConfig);
                }

                // create sqs-specific cloud storage instance for dropped requests (different bucket)
                String optoutBucketDroppedRequests = this.config.getString(Const.Config.OptOutS3BucketDroppedRequestsProp);
                ICloudStorage fsSqsDroppedRequests = CloudUtils.createStorage(optoutBucketDroppedRequests, config);

                // deploy sqs log producer with its own storage instances 
                OptOutSqsLogProducer sqsLogProducer = new OptOutSqsLogProducer(this.config, fsSqs, fsSqsDroppedRequests, sqsCs, Const.Event.DeltaProduce, null);
                futs.add(this.deploySingleInstance(sqsLogProducer));

                LOGGER.info("SQS log producer deployed - bucket: {}, folder: {}", 
                    this.config.getString(Const.Config.OptOutS3BucketProp), sqsFolder);
            } catch (IOException e) {
                LOGGER.error("circuit_breaker_config_error: failed to initialize SQS log producer, delta production will be disabled: {}", e.getMessage(), e);
            } catch (MalformedTrafficFilterConfigException e) {
                LOGGER.error("circuit_breaker_config_error: traffic filter config is malformed, delta production will be disabled: {}", e.getMessage(), e);
            } catch (MalformedTrafficCalcConfigException e) {
                LOGGER.error("circuit_breaker_config_error: traffic calc config is malformed, delta production will be disabled: {}", e.getMessage(), e);
            }
        }

        Supplier<Verticle> svcSupplier = () -> {
            OptOutServiceVerticle svc = new OptOutServiceVerticle(vertx, this.operatorKeyProvider, this.fsOptOut, this.config, this.uidInstanceIdProvider);
            // configure where OptOutService receives the latest cloud paths
            cs.registerNewCloudPathsHandler(ps -> svc.setCloudPaths(ps));
            return svc;
        };

        LOGGER.info("Deploying config stores...");
        int svcInstances = this.config.getInteger(Const.Config.ServiceInstancesProp);
        CompositeFuture.all(futs)
            .compose(v -> {
                LOGGER.info("Config stores deployed, deploying service instances...");
                return this.deploy(svcSupplier, svcInstances);
            })
            .compose(v -> {
                LOGGER.info("Service instances deployed, setting up timers...");
                return setupTimerEvents(cloudSyncVerticle.eventRefresh());
            })
            .onSuccess(v -> {
                LOGGER.info("OptOut service fully started...");
            })
            .onFailure(t -> {
                LOGGER.error("Unable to bootstrap OptOutService and its dependencies");
                LOGGER.error(t.getMessage(), new Exception(t));
                vertx.close();
                System.exit(1);
            });
    }

    private Future uploadLastDelta(OptOutCloudSync cs, OptOutLogProducer logProducer, String eventUpload, String eventRefresh) {
        final String deltaLocalPath;
        try {
            deltaLocalPath = logProducer.getLastDelta();
            // no need to upload if delta cannot be found
            if (deltaLocalPath == null) {
                LOGGER.info("found no last delta on disk");
                return Future.succeededFuture();
            }
        } catch (Exception ex) {
            LOGGER.error("uploadLastDelta error: " + ex.getMessage(), ex);
            return Future.failedFuture(ex);
        }

        Promise<Void> promise = Promise.promise();
        AtomicReference<Object> handler = new AtomicReference<>();
        handler.set(cs.registerNewCloudPathsHandler(cloudPaths -> {
            try {
                cs.unregisterNewCloudPathsHandler(handler.get());
                final String deltaCloudPath = cs.toCloudPath(deltaLocalPath);
                if (cloudPaths.contains(deltaCloudPath)) {
                    // if delta is already uploaded, the work is already done
                    LOGGER.info("found no last delta that needs to be uploaded");
                } else {
                    this.fsOptOut.upload(deltaLocalPath, deltaCloudPath);
                    LOGGER.warn("found last delta that is not uploaded " + deltaLocalPath);
                    LOGGER.warn("uploaded last delta to " + deltaCloudPath);
                }
                promise.complete();
            } catch (Exception ex) {
                final String msg = "unable handle last delta upload: " + ex.getMessage();
                LOGGER.error(msg, ex);
                promise.fail(new Exception(msg, ex));
            }
        }));

        // refresh now to mitigate a race-condition (cloud refreshed before cloudPaths handler is registered)
        vertx.eventBus().send(eventRefresh, 0);

        AtomicInteger counter = new AtomicInteger(0);
        vertx.setPeriodic(60*1000, id -> {
            if (HealthManager.instance.isHealthy()) {
                vertx.cancelTimer(id);
                return;
            }

            int count = counter.incrementAndGet();
            if (count >= 10) {
                LOGGER.error("Unable to refresh from cloud storage and upload last delta...");
                vertx.close();
                System.exit(1);
                return;
            }

            LOGGER.warn("Waiting for cloud refresh to complete. Sending " + count + " " + eventRefresh + "...");
            vertx.eventBus().send(eventRefresh, 0);
        });

        return promise.future();
    }

    private Future<String> createOperatorKeyRotator() {
        RotatingStoreVerticle rotatingStore = new RotatingStoreVerticle("operators", 10000, operatorKeyProvider);
        return this.deploySingleInstance(rotatingStore);
    }

    private Future<String> createPartnerConfigMonitor(String eventCloudSyncDownloaded) {
        if (config.getString(Const.Config.PartnersMetadataPathProp) != null) {
            return createPartnerConfigMonitorV2(eventCloudSyncDownloaded);
        }

        String partnerConfigPath = config.getString(Const.Config.PartnersConfigPathProp);
        if (partnerConfigPath == null || partnerConfigPath.length() == 0)
            return Future.succeededFuture();
        PartnerConfigMonitor configMon = new PartnerConfigMonitor(vertx, config, fsPartnerConfig, eventCloudSyncDownloaded);
        RotatingStoreVerticle rotatingStore = new RotatingStoreVerticle("partners", 10000, configMon);
        return this.deploySingleInstance(rotatingStore);
    }

    private Future<String> createPartnerConfigMonitorV2(String eventCloudSyncDownloaded) {
        final DownloadCloudStorage fsMetadata, fsContent;
        if (this.fsOperatorKeyConfig instanceof UidCoreClient) {
            fsMetadata = this.fsOperatorKeyConfig;
            fsContent = ((UidCoreClient)this.fsOperatorKeyConfig).getContentStorage();
        } else {
            fsMetadata = this.fsOperatorKeyConfig;
            fsContent = this.fsOperatorKeyConfig;
        }

        PartnerConfigMonitorV2 configMon = new PartnerConfigMonitorV2(vertx, config, fsMetadata, fsContent, eventCloudSyncDownloaded);
        RotatingStoreVerticle rotatingStore = new RotatingStoreVerticle("partners", 10000, configMon);
        return this.deploySingleInstance(rotatingStore);
    }

    private void createAppStatusMetric() {
        String version = Optional.ofNullable(System.getenv("IMAGE_VERSION")).orElse("unknown");
        Gauge.builder("app_status", () -> 1)
            .description("application version and status")
            .tag("version", version)
            .register(Metrics.globalRegistry);
    }

    private void createVertxInstancesMetric() {
        Gauge.builder("uid2_vertx_service_instances", () -> config.getInteger("service_instances"))
            .description("gauge for number of vertx service instances requested")
            .register(Metrics.globalRegistry);
    }

    private void createVertxEventLoopsMetric() {
        Gauge.builder("uid2_vertx_event_loop_threads", () -> VertxOptions.DEFAULT_EVENT_LOOP_POOL_SIZE)
            .description("gauge for number of vertx event loop threads")
            .register(Metrics.globalRegistry);
    }

    private Future<String> deploySingleInstance(AbstractVerticle verticle) {
        return this.deploy(() -> verticle, 1);
    }

    private Future<String> deploy(Supplier<Verticle> verticleSupplier, int numInstances) {
        Promise<String> promise = Promise.promise();

        // set correct number of instances when deploying
        DeploymentOptions options = new DeploymentOptions();
        options.setInstances(numInstances);

        vertx.deployVerticle(verticleSupplier, options, ar -> promise.handle(ar));
        return promise.future();
    }

    private Future<Void> setupTimerEvents(String eventCloudRefresh) {
        // refresh now to ready optout service verticles
        vertx.eventBus().send(eventCloudRefresh, 0);

        int rotateInterval = config.getInteger(Const.Config.OptOutDeltaRotateIntervalProp);
        int cloudRefreshInterval = config.getInteger(Const.Config.CloudRefreshIntervalProp);

        // if we plan to consolidate logs from multiple replicas, we need to make sure they are produced at roughly
        // the same time, e.g. if the logs are produced every 5 mins, ideally we'd like to send log.produce event
        // at 00, 05, 10, 15 mins etc, of each hour.
        //
        // first calculate seconds to sleep to get to the above exact intervals
        final int secondsToSleep = OptOutUtils.getSecondsBeforeNextSlot(Instant.now(), rotateInterval);
        final int msToSleep = secondsToSleep > 0 ? secondsToSleep * 1000 : 1;
        LOGGER.info("sleep for " + secondsToSleep + "s before scheduling the first log rotate event");
        vertx.setTimer(msToSleep, v -> {
            // at the right starting time, start periodically emitting log.produce event
            vertx.setPeriodic(1000 * rotateInterval, id -> {
                LOGGER.trace("sending " + Const.Event.DeltaProduce);
                vertx.eventBus().send(Const.Event.DeltaProduce, id);
            });
        });

        // add 15s offset to do s3 refresh also synchronized
        final int secondsToSleep2 = (secondsToSleep + 15) % cloudRefreshInterval;
        final int msToSleep2 = secondsToSleep2 > 0 ? secondsToSleep2 * 1000 : 1;
        LOGGER.info("sleep for " + secondsToSleep2 + "s before scheduling the first s3 refresh event");
        vertx.setTimer(msToSleep2, v -> {
            LOGGER.info("sending the 1st " + eventCloudRefresh);
            vertx.eventBus().send(eventCloudRefresh, -1);

            // periodically emit s3.refresh event
            vertx.setPeriodic(1000 * cloudRefreshInterval, id -> {
                LOGGER.trace("sending " + eventCloudRefresh);
                vertx.eventBus().send(eventCloudRefresh, id);
            });
        });

        return Future.succeededFuture();
    }
}
