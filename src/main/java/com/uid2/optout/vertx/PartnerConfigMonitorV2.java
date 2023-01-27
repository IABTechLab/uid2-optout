package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.partner.EndpointConfig;
import com.uid2.optout.partner.PartnersConfig;
import com.uid2.shared.Utils;
import com.uid2.shared.auth.ClientKey;
import com.uid2.shared.cloud.DownloadCloudStorage;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.store.reader.IMetadataVersionedStore;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class PartnerConfigMonitorV2 implements IMetadataVersionedStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartnerConfigMonitorV2.class);

    private final Vertx vertx;
    private final JsonObject globalConfig;
    private final DownloadCloudStorage metadataStorage;
    private final DownloadCloudStorage contentStorage;
    private final String partnersMetadataPath;
    private final String eventCloudDownloaded;
    private final AtomicReference<Map<String, String>> senderDeploymentIds = new AtomicReference<>();

    public PartnerConfigMonitorV2(Vertx vertx, JsonObject globalConfig, DownloadCloudStorage metadataStorage,
                                  DownloadCloudStorage contentStorage, String eventCloudDownloaded) {
        this.vertx = vertx;
        this.globalConfig = globalConfig;
        this.metadataStorage = metadataStorage;
        this.contentStorage = contentStorage;
        this.partnersMetadataPath = globalConfig.getString(Const.Config.PartnersMetadataPathProp);;
        this.eventCloudDownloaded = eventCloudDownloaded;
    }

    @Override
    public JsonObject getMetadata() throws Exception {
        InputStream s = this.metadataStorage.download(this.partnersMetadataPath);
        return Utils.toJsonObject(s);
    }

    @Override
    public long getVersion(JsonObject metadata) {
        return metadata.getLong("version");
    }

    @Override
    public long loadContent(JsonObject metadata) throws Exception {
        final JsonObject partnersMetadata = metadata.getJsonObject("partners");
        final String path = partnersMetadata.getString("location");
        final InputStream inputStream = this.contentStorage.download(path);
        JsonArray endpointsSpec = Utils.toJsonArray(inputStream);
        List<EndpointConfig> remoteEndpoints = new ArrayList<>();
        for (int i = 0; i < endpointsSpec.size(); ++i) {
            JsonObject endpointSpec = endpointsSpec.getJsonObject(i);
            EndpointConfig remoteEndpoint = EndpointConfig.valueOf(endpointSpec);
            remoteEndpoints.add(remoteEndpoint);
        }

        int totalEndpoints = remoteEndpoints.size();
        int uniqueNames = remoteEndpoints.stream()
            .map(e -> e.name())
            .collect(Collectors.toSet()).size();
        if (totalEndpoints > uniqueNames) {
            throw new Exception("Unable to undeploy new partners config due to name conflict");
        }

        Map<String, String> deployIdMap = this.senderDeploymentIds.get();
        List<Future> fs = new ArrayList<>();
        if (deployIdMap != null) {
            for (String deployId : deployIdMap.values()) {
                LOGGER.info("Undeploying old OptOutSender: " + deployId);
                Promise<Void> promise = Promise.promise();
                vertx.undeploy(deployId, ar -> promise.handle(ar));
                fs.add(promise.future());
            }
        } else {
            this.senderDeploymentIds.set(deployIdMap);
            fs.add(Future.succeededFuture());
        }

        final Map<String, String> newDeployIdMap = new HashMap<>();
        this.senderDeploymentIds.set(newDeployIdMap);

        CompositeFuture.all(fs).onComplete(ar -> {
            if (ar.failed()) {
                Throwable e = ar.cause();
                LOGGER.error("Unable to undeploy old OptOutSender: " + e.getMessage(), e);
                return;
            }

            for (EndpointConfig ef : remoteEndpoints) {
                LOGGER.info("Deploying OptOutSender: " + ef.name() + ", url: " + ef.url());
                OptOutSender sender = new OptOutSender(globalConfig, vertx, ef, this.eventCloudDownloaded);
                vertx.deployVerticle(sender, dr -> {
                    if (dr.succeeded()) {
                        newDeployIdMap.put(ef.name(), dr.result());
                    } else {
                        LOGGER.fatal("Failed to deploy OptOutSender: " + ef.name());
                    }
                });
            }
        });

        return remoteEndpoints.size();
    }

    public void runOnce() throws Exception {
        this.loadContent(this.getMetadata());
    }
}
