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
import com.uid2.shared.jmx.AdminApi;
import com.uid2.shared.optout.OptOutCloudSync;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Supplier;

//
// produces events:
//   - cloudsync.optout.refresh (timer-based)
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
    private final UidInstanceIdProvider uidInstanceIdProvider;

    public Main(Vertx vertx, JsonObject config) throws Exception {
        this.vertx = vertx;
        this.config = config;
        this.observeOnly = config.getBoolean(Const.Config.OptOutObserveOnlyProp);
        if (this.observeOnly) {
            LOGGER.warn("Running Observe ONLY mode: no producer, no sender");
        }
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

        // main CloudSyncVerticle - downloads from optout_s3_folder (for readers: partition generator, log sender)
        OptOutCloudSync cs = new OptOutCloudSync(this.config, true);
        CloudSyncVerticle cloudSyncVerticle = new CloudSyncVerticle("optout", this.fsOptOut, this.fsLocal, cs, this.config);

        // deploy optout cloud sync verticle
        futs.add(this.deploySingleInstance(cloudSyncVerticle));

        // deploy operator key rotator
        futs.add(this.createOperatorKeyRotator());

        if (!this.observeOnly) {
            // enable partition producing (reads from main folder via cs)
            cs.enableDeltaMerging(vertx, Const.Event.PartitionProduce);

            // deploy partition producer verticle - uploads to S3 via CloudSyncVerticle
            OptOutLogProducer partitionProducer = new OptOutLogProducer(this.config, cloudSyncVerticle.eventUpload());
            futs.add(this.deploySingleInstance(partitionProducer));

            // create partners config monitor
            futs.add(this.createPartnerConfigMonitor(cloudSyncVerticle.eventDownloaded()));

            // deploy sqs log producer
            try {
                // Create default config files for local/e2e testing if needed
                createDefaultConfigFilesIfNeeded();

                String optoutBucketDroppedRequests = this.config.getString(Const.Config.OptOutS3BucketDroppedRequestsProp);
                ICloudStorage fsSqsDroppedRequests = CloudUtils.createStorage(optoutBucketDroppedRequests, this.config);

                // deploy sqs log producer
                OptOutSqsLogProducer sqsLogProducer = new OptOutSqsLogProducer(this.config, this.fsOptOut, fsSqsDroppedRequests, cs, Const.Event.DeltaProduced, null);
                futs.add(this.deploySingleInstance(sqsLogProducer));

                LOGGER.info("sqs log producer deployed, bucket={}, folder={}", 
                    this.config.getString(Const.Config.OptOutS3BucketProp), 
                    this.config.getString(Const.Config.OptOutS3FolderProp));
            } catch (IOException e) {
                LOGGER.error("circuit_breaker_config_error: failed to initialize sqs log producer, delta production will be disabled: {}", e.getMessage(), e);
            } catch (MalformedTrafficFilterConfigException e) {
                LOGGER.error("circuit_breaker_config_error: traffic filter config is malformed, delta production will be disabled: {}", e.getMessage(), e);
            } catch (MalformedTrafficCalcConfigException e) {
                LOGGER.error("circuit_breaker_config_error: traffic calc config is malformed, delta production will be disabled: {}", e.getMessage(), e);
            }
        }

        Supplier<Verticle> svcSupplier = () -> {
            OptOutServiceVerticle svc = new OptOutServiceVerticle(vertx, this.operatorKeyProvider, this.fsOptOut, this.config);
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
        PartnerConfigMonitor configMon = new PartnerConfigMonitor(vertx, config, fsPartnerConfig, eventCloudSyncDownloaded, fsOptOut);
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

        PartnerConfigMonitorV2 configMon = new PartnerConfigMonitorV2(vertx, config, fsMetadata, fsContent, eventCloudSyncDownloaded, fsOptOut);
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

        int cloudRefreshInterval = config.getInteger(Const.Config.CloudRefreshIntervalProp);

        // periodically emit s3.refresh event
        vertx.setPeriodic(1000 * cloudRefreshInterval, id -> {
            LOGGER.trace("sending " + eventCloudRefresh);
            vertx.eventBus().send(eventCloudRefresh, id);
        });

        return Future.succeededFuture();
    }

    /**
     * Creates default traffic filter and traffic calc config files for local/e2e testing.
     * Only creates files if running in non-production mode and config paths are not set.
     */
    private void createDefaultConfigFilesIfNeeded() {
        if (Utils.isProductionEnvironment()) {
            return;
        }

        try {
            Path tempDir = Files.createTempDirectory("optout-config");
            
            // Create traffic filter config if not set
            String filterPath = config.getString(Const.Config.TrafficFilterConfigPathProp);
            if (filterPath == null || filterPath.isEmpty()) {
                Path filterConfigPath = tempDir.resolve("traffic-filter-config.json");
                String filterConfig = "{\n  \"denylist_requests\": []\n}";
                Files.writeString(filterConfigPath, filterConfig);
                config.put(Const.Config.TrafficFilterConfigPathProp, filterConfigPath.toString());
                LOGGER.info("Created default traffic filter config at: {}", filterConfigPath);
            }

            // Create traffic calc config if not set
            String calcPath = config.getString(Const.Config.TrafficCalcConfigPathProp);
            if (calcPath == null || calcPath.isEmpty()) {
                Path calcConfigPath = tempDir.resolve("traffic-calc-config.json");
                String calcConfig = "{\n" +
                    "  \"traffic_calc_evaluation_window_seconds\": 86400,\n" +
                    "  \"traffic_calc_baseline_traffic\": 1000000,\n" +
                    "  \"traffic_calc_threshold_multiplier\": 10,\n" +
                    "  \"traffic_calc_allowlist_ranges\": []\n" +
                    "}";
                Files.writeString(calcConfigPath, calcConfig);
                config.put(Const.Config.TrafficCalcConfigPathProp, calcConfigPath.toString());
                LOGGER.info("Created default traffic calc config at: {}", calcConfigPath);
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to create default config files: {}", e.getMessage());
        }
    }
}
