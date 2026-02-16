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

    private AutoCloseable mocks;
    @Mock
    private IOptOutPartnerEndpoint optOutPartnerEndpoint;
    private final String partnerName = "testPartner";
    private final String filePath = "/tmp/uid2/optout";
    private final String eventBusName = "testEventBus";
    private CompletableFuture<Void> test;
    private OptOutSender optoutSender;
    private final JsonObject config = new JsonObject();
    private InMemoryStorageMock cloudStorage;

    private SimpleMeterRegistry registry;

    private static final String S3_TIMESTAMP_KEY = "optout/sender-state/testPartner_timestamp.txt";
    private static final String S3_PROCESSED_KEY = "optout/sender-state/testPartner_processed.txt";

    @BeforeEach
    public void setup() {
        new File(filePath + "/consumer/delta").mkdirs();
    }

    /**
     * Deploy the verticle and block until deployment is complete.
     * This ensures the event bus consumer is registered and state is loaded before the test continues.
     */
    private void deployAndAwait(Vertx vertx) throws Exception {
        deployAndAwait(vertx, new InMemoryStorageMock());
    }

    private void deployAndAwait(Vertx vertx, InMemoryStorageMock storage) throws Exception {
        mocks = MockitoAnnotations.openMocks(this);
        setupConfig();
        setupMocks(vertx);

        this.cloudStorage = storage;
        this.optoutSender = new OptOutSender(config, optOutPartnerEndpoint, eventBusName, this.cloudStorage);

        CompletableFuture<String> deployFuture = new CompletableFuture<>();
        vertx.deployVerticle(optoutSender, ar -> {
            if (ar.succeeded()) deployFuture.complete(ar.result());
            else deployFuture.completeExceptionally(ar.cause());
        });
        deployFuture.get(10, TimeUnit.SECONDS);

        this.registry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(registry);
    }

    private void setupMocks(Vertx vertx) {
        when(optOutPartnerEndpoint.name()).thenReturn(partnerName);
        test = new CompletableFuture<>();
        when(optOutPartnerEndpoint.send(any())).then((a) -> {
            test.complete(null);
            return Future.fromCompletionStage(test, vertx.getOrCreateContext());
        });
    }

    private void setupConfig() {
        config.put(Const.Config.OptOutDataDirProp, filePath);
        config.put(Const.Config.OptOutProducerReplicaIdProp, 1);

        config.put(Const.Config.OptOutSenderReplicaIdProp, 1);
        config.put(Const.Config.OptOutProducerMaxReplicasProp, 1);

        config.put(Const.Config.OptOutDeltaRotateIntervalProp, 300);
    }

    @AfterEach
    public void cleanup() throws IOException {
        Files.walk(Paths.get(filePath))
                .map(Path::toFile)
                .forEach(File::delete);
    }

    private Path getDeltaPath() {
        return Paths.get(filePath, "consumer/delta", "optout-delta-" + TestUtils.newSuffix());
    }

    private String readS3String(String key) throws Exception {
        try (InputStream is = cloudStorage.download(key)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
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
        vertx.eventBus().publish(eventBusName, newFile.toString());

        test.get(10, TimeUnit.SECONDS);
        verify(optOutPartnerEndpoint, times(3)).send(any());
        testContext.completeNow();
    }

    @Test
    void testStatePersistedToS3AfterProcessing(Vertx vertx, VertxTestContext testContext) throws Exception {
        deployAndAwait(vertx);
        Path newFile = getDeltaPath();
        TestUtils.newDeltaFile(newFile, 1, 2, 3);
        vertx.eventBus().publish(eventBusName, newFile.toString());

        test.get(10, TimeUnit.SECONDS);

        // Allow time for the async S3 persist to complete
        Thread.sleep(2000);

        String timestampContent = readS3String(S3_TIMESTAMP_KEY);
        assertNotNull(timestampContent, "Timestamp should be persisted to S3");
        assertFalse(timestampContent.isBlank(), "Timestamp in S3 should not be blank");
        long timestamp = Long.parseLong(timestampContent.trim());
        assertTrue(timestamp > 0, "Timestamp should be a positive epoch second");

        String processedContent = readS3String(S3_PROCESSED_KEY);
        assertNotNull(processedContent, "Processed deltas should be persisted to S3");
        assertTrue(processedContent.contains(newFile.toString()),
                "S3 processed deltas should contain the delta file that was sent");

        testContext.completeNow();
    }

    @Test
    void testStateRecoveredFromS3OnRestart(Vertx vertx, VertxTestContext testContext) throws Exception {
        InMemoryStorageMock sharedStorage = new InMemoryStorageMock();
        deployAndAwait(vertx, sharedStorage);

        Path newFile = getDeltaPath();
        TestUtils.newDeltaFile(newFile, 1, 2, 3);
        vertx.eventBus().publish(eventBusName, newFile.toString());

        test.get(10, TimeUnit.SECONDS);

        // Allow time for the async S3 persist to complete
        Thread.sleep(2000);

        // Verify state was persisted to S3
        String timestampBefore = readS3String(S3_TIMESTAMP_KEY);
        assertNotNull(timestampBefore);
        long savedTimestamp = Long.parseLong(timestampBefore.trim());
        assertTrue(savedTimestamp > 0);

        String processedBefore = readS3String(S3_PROCESSED_KEY);
        assertTrue(processedBefore.contains(newFile.toString()));

        // Undeploy (simulating pod termination)
        CompletableFuture<Void> undeployFuture = new CompletableFuture<>();
        vertx.undeploy(optoutSender.deploymentID(), ar -> {
            if (ar.succeeded()) undeployFuture.complete(null);
            else undeployFuture.completeExceptionally(ar.cause());
        });
        undeployFuture.get(10, TimeUnit.SECONDS);

        // Redeploy with the same S3 storage (simulating fresh pod with no persistent volume)
        test = new CompletableFuture<>();
        when(optOutPartnerEndpoint.send(any())).then((a) -> {
            test.complete(null);
            return Future.fromCompletionStage(test, vertx.getOrCreateContext());
        });

        this.optoutSender = new OptOutSender(config, optOutPartnerEndpoint, eventBusName, sharedStorage);
        CompletableFuture<String> redeployFuture = new CompletableFuture<>();
        vertx.deployVerticle(optoutSender, ar -> {
            if (ar.succeeded()) redeployFuture.complete(ar.result());
            else redeployFuture.completeExceptionally(ar.cause());
        });
        redeployFuture.get(10, TimeUnit.SECONDS);

        // Verify the S3 state survives — the same keys should still have the same content
        String timestampAfter = readS3String(S3_TIMESTAMP_KEY);
        assertEquals(timestampBefore.trim(), timestampAfter.trim(),
                "Timestamp in S3 should survive across pod restarts");

        String processedAfter = readS3String(S3_PROCESSED_KEY);
        assertTrue(processedAfter.contains(newFile.toString()),
                "Processed deltas in S3 should survive across pod restarts");

        testContext.completeNow();
    }
}
