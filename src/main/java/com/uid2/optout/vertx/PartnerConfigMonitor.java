package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.partner.EndpointConfig;
import com.uid2.optout.partner.PartnersConfig;
import com.uid2.shared.Utils;
import com.uid2.shared.audit.UidInstanceIdProvider;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.store.reader.IMetadataVersionedStore;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class PartnerConfigMonitor implements IMetadataVersionedStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartnerConfigMonitor.class);

    private final Vertx vertx;
    private final JsonObject globalConfig;
    private final ICloudStorage cloudStorage;
    private final String partnersConfigPath;
    private final String eventCloudDownloaded;
    private final AtomicReference<Map<String, String>> senderDeploymentIds = new AtomicReference<>();
    private final UidInstanceIdProvider uidInstanceIdProvider;

    public PartnerConfigMonitor(Vertx vertx,JsonObject globalConfig, ICloudStorage cloudStorage, String eventCloudDownloaded, UidInstanceIdProvider uidInstanceIdProvider) {
        this.vertx = vertx;
        this.globalConfig = globalConfig;
        this.cloudStorage = cloudStorage;
        this.partnersConfigPath = globalConfig.getString(Const.Config.PartnersConfigPathProp);;
        this.eventCloudDownloaded = eventCloudDownloaded;
        this.uidInstanceIdProvider = uidInstanceIdProvider;
    }

    @Override
    public JsonObject getMetadata() throws Exception {
        try (InputStream s = this.cloudStorage.download(this.partnersConfigPath)) {
            return Utils.toJsonObject(s);
        }
    }

    @Override
    public long getVersion(JsonObject metadata) {
        return metadata.getLong("version");
    }

    @Override
    public long loadContent(JsonObject metadata) throws Exception {
        PartnersConfig config = PartnersConfig.fromJsonObject(metadata);
        int totalEndpoints = config.remoteEndpoints().size();
        int uniqueNames = config.remoteEndpoints().stream()
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

            for (EndpointConfig ef : config.remoteEndpoints()) {
                LOGGER.info("Deploying OptOutSender: " + ef.name() + ", url: " + ef.url());
                OptOutSender sender = new OptOutSender(globalConfig, vertx, ef, this.eventCloudDownloaded, this.uidInstanceIdProvider);
                vertx.deployVerticle(sender, dr -> {
                    if (dr.succeeded()) {
                        newDeployIdMap.put(ef.name(), dr.result());
                    } else {
                        LOGGER.error("Failed to deploy OptOutSender: " + ef.name());
                    }
                });
            }
        });

        return config.remoteEndpoints().size();
    }

    public void runOnce() throws Exception {
        this.loadContent(this.getMetadata());
    }
}
