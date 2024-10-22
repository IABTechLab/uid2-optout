package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.TestUtils;
import com.uid2.optout.partner.IOptOutPartnerEndpoint;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.uid2.shared.optout.OptOutEntry;
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

    private SimpleMeterRegistry registry;

    @BeforeEach
    public void setup() {
        new File(filePath + "/consumer/delta").mkdirs();
    }

    public void deployVerticle(Vertx vertx, VertxTestContext testContext) {
        mocks = MockitoAnnotations.openMocks(this);

        setupConfig();
        setupMocks(vertx);

        this.optoutSender = new OptOutSender(config, optOutPartnerEndpoint, eventBusName);

        vertx.deployVerticle(optoutSender, testContext.succeeding(id -> testContext.completeNow()));

        this.registry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(registry);
    }

    private void setupMocks(Vertx vertx) {
        when(optOutPartnerEndpoint.name()).thenReturn(partnerName);
        test = new CompletableFuture<Void>();
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


    // Also tests ScanLocalForUnprocessedWithNoNewFiles
    @Test
    void verticleDeployed(Vertx vertx, VertxTestContext testContext) {
        deployVerticle(vertx, testContext);
        testContext.completeNow();
    }

    @Test
    void testScanLocalForUnprocessedWithNewFile(Vertx vertx, VertxTestContext testContext) {
        TestUtils.newDeltaFile(getDeltaPath(), 1, 2, 3);
        deployVerticle(vertx, testContext);
        testContext.completeNow();
    }

    // If this test hangs delete the /tmp/uid2/optout folder and run again.
    @Test
    void testRecieveMessageAndSendsIDs(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        deployVerticle(vertx, testContext);
        Path newFile = getDeltaPath();
        TestUtils.newDeltaFile(newFile, 1, 2, 3);
        vertx.eventBus().publish(eventBusName, newFile.toString());

        while(!test.isDone()) {
            Thread.sleep(100);
        }
        verify(optOutPartnerEndpoint, times(3)).send(any());
        testContext.completeNow();
    }
}
