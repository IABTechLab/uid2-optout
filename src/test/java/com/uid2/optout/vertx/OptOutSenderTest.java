package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.TestUtils;
import com.uid2.optout.partner.IOptOutPartnerEndpoint;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.uid2.shared.cloud.InMemoryStorageMock;
import com.uid2.shared.health.HealthManager;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
public class OptOutSenderTest {

    @Mock
    private IOptOutPartnerEndpoint optOutPartnerEndpoint;
    private final String partnerName = "testPartner";
    private final String filePath = "/tmp/uid2/optout";
    private final String eventBusName = "testEventBus";
    private final JsonObject config = new JsonObject();
    private OptOutSender optoutSender;
    private InMemoryStorageMock cloudStorage;

    private static final String CLOUD_TIMESTAMP_KEY = "optout/sender-state/testPartner_timestamp.txt";
    private static final String CLOUD_PROCESSED_KEY = "optout/sender-state/testPartner_processed.txt";
    private static final int TIMEOUT_SECONDS = 10;

    @BeforeEach
    public void setup() {
        new File(filePath + "/consumer/delta").mkdirs();
    }

    @AfterEach
    public void cleanup() throws IOException {
        HealthManager.instance.reset();
        Files.walk(Paths.get(filePath))
                .map(Path::toFile)
                .forEach(File::delete);
    }

    private void deployAndAwait(Vertx vertx) throws Exception {
        deployAndAwait(vertx, new InMemoryStorageMock());
    }

    private void deployAndAwait(Vertx vertx, InMemoryStorageMock storage) throws Exception {
        MockitoAnnotations.openMocks(this);
        setupConfig();
        when(optOutPartnerEndpoint.name()).thenReturn(partnerName);
        when(optOutPartnerEndpoint.send(any())).thenReturn(Future.succeededFuture());

        this.cloudStorage = storage;
        this.optoutSender = new OptOutSender(config, optOutPartnerEndpoint, eventBusName, this.cloudStorage);
        awaitFuture(vertx.deployVerticle(optoutSender));

        Metrics.globalRegistry.add(new SimpleMeterRegistry());
    }

    private void setupConfig() {
        config.put(Const.Config.OptOutDataDirProp, filePath);
        config.put(Const.Config.OptOutProducerReplicaIdProp, 1);
        config.put(Const.Config.OptOutSenderReplicaIdProp, 1);
        config.put(Const.Config.OptOutProducerMaxReplicasProp, 1);
        config.put(Const.Config.OptOutDeltaRotateIntervalProp, 300);
    }

    private static <T> T awaitFuture(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private Path getDeltaPath() {
        return Paths.get(filePath, "consumer/delta", "optout-delta-" + TestUtils.newSuffix());
    }

    private String readCloudString(String key) throws Exception {
        try (InputStream is = cloudStorage.download(key)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void publishAndAwaitSent(Vertx vertx, Path deltaFile) throws Exception {
        CompletableFuture<Void> sentFuture = new CompletableFuture<>();
        vertx.eventBus().<String>consumer(Const.Event.DeltaSentRemote, msg -> sentFuture.complete(null));
        vertx.eventBus().publish(eventBusName, deltaFile.toString());
        sentFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }


    @Test
    void verticleDeployed(Vertx vertx, VertxTestContext testContext) throws Exception {
        deployAndAwait(vertx);
        testContext.completeNow();
    }

    @Test
    void testScanLocalForUnprocessedWithNewFile(Vertx vertx, VertxTestContext testContext) throws Exception {
        TestUtils.newDeltaFile(getDeltaPath(), 1, 2, 3);
        deployAndAwait(vertx);
        testContext.completeNow();
    }

    @Test
    void testRecieveMessageAndSendsIDs(Vertx vertx, VertxTestContext testContext) throws Exception {
        deployAndAwait(vertx);
        Path newFile = getDeltaPath();
        TestUtils.newDeltaFile(newFile, 1, 2, 3);

        publishAndAwaitSent(vertx, newFile);

        verify(optOutPartnerEndpoint, times(3)).send(any());
        testContext.completeNow();
    }

    @Test
    void testStatePersistedToCloudAfterProcessing(Vertx vertx, VertxTestContext testContext) throws Exception {
        deployAndAwait(vertx);
        Path newFile = getDeltaPath();
        TestUtils.newDeltaFile(newFile, 1, 2, 3);

        publishAndAwaitSent(vertx, newFile);

        // Allow time for the async cloud persist to complete
        Thread.sleep(2000);

        String timestampContent = readCloudString(CLOUD_TIMESTAMP_KEY);
        assertNotNull(timestampContent, "Timestamp should be persisted to cloud storage");
        assertFalse(timestampContent.isBlank(), "Timestamp in cloud storage should not be blank");
        long timestamp = Long.parseLong(timestampContent.trim());
        assertTrue(timestamp > 0, "Timestamp should be a positive epoch second");

        String processedContent = readCloudString(CLOUD_PROCESSED_KEY);
        assertNotNull(processedContent, "Processed deltas should be persisted to cloud storage");
        assertTrue(processedContent.contains(newFile.toString()),
                "Cloud processed deltas should contain the delta file that was sent");

        testContext.completeNow();
    }

    @Test
    void testStateRecoveredFromCloudOnRestart(Vertx vertx, VertxTestContext testContext) throws Exception {
        InMemoryStorageMock sharedStorage = new InMemoryStorageMock();
        deployAndAwait(vertx, sharedStorage);

        Path newFile = getDeltaPath();
        TestUtils.newDeltaFile(newFile, 1, 2, 3);

        publishAndAwaitSent(vertx, newFile);

        // Allow time for the async cloud persist to complete
        Thread.sleep(2000);

        String timestampBefore = readCloudString(CLOUD_TIMESTAMP_KEY);
        assertNotNull(timestampBefore);
        assertTrue(Long.parseLong(timestampBefore.trim()) > 0);

        String processedBefore = readCloudString(CLOUD_PROCESSED_KEY);
        assertTrue(processedBefore.contains(newFile.toString()));

        // Undeploy and redeploy with the same cloud storage (simulating fresh pod)
        awaitFuture(vertx.undeploy(optoutSender.deploymentID()));

        when(optOutPartnerEndpoint.send(any())).thenReturn(Future.succeededFuture());
        this.optoutSender = new OptOutSender(config, optOutPartnerEndpoint, eventBusName, sharedStorage);
        awaitFuture(vertx.deployVerticle(optoutSender));

        // Verify the cloud state survives across pod restarts
        assertEquals(timestampBefore.trim(), readCloudString(CLOUD_TIMESTAMP_KEY).trim(),
                "Timestamp in cloud storage should survive across pod restarts");
        assertTrue(readCloudString(CLOUD_PROCESSED_KEY).contains(newFile.toString()),
                "Processed deltas in cloud storage should survive across pod restarts");

        testContext.completeNow();
    }
}
