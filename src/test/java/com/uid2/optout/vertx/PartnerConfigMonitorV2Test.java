package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.shared.cloud.CloudStorageException;
import com.uid2.shared.cloud.DownloadCloudStorage;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class PartnerConfigMonitorV2Test {
    private AutoCloseable mocks;
    private final String filePath = "/tmp/uid2/optout";
    private final JsonObject config = new JsonObject();
    private final String eventBusName = "testEventBus";

    @Mock
    private DownloadCloudStorage metadataStorage;
    @Mock
    private DownloadCloudStorage contentStorage;

    private PartnerConfigMonitorV2 partnerConfigMonitorV2;

    @BeforeEach
    public void deployVerticle(Vertx vertx, VertxTestContext testContext) {
        mocks = MockitoAnnotations.openMocks(this);

        setupConfig();

        partnerConfigMonitorV2 = new PartnerConfigMonitorV2(vertx, config, metadataStorage, contentStorage, eventBusName);
        testContext.completeNow();
    }

    private void setupConfig() {
        config.put(Const.Config.OptOutDataDirProp, filePath);
        config.put(Const.Config.OptOutProducerReplicaIdProp, 1);

        config.put(Const.Config.OptOutSenderReplicaIdProp, 1);
        config.put(Const.Config.OptOutProducerMaxReplicasProp, 1);

        config.put(Const.Config.OptOutDeltaRotateIntervalProp, 300);

        config.put(Const.Config.PartnersMetadataPathProp, "testPath");
    }

    @Test
    void testConstructor(Vertx vertx, VertxTestContext testContext) {
        testContext.completeNow();
    }

    @Test
    void testLoadContent(Vertx vertx, VertxTestContext testContext) throws Exception {
        JsonObject metadata = new JsonObject();
        JsonObject partner = new JsonObject();
        partner.put("location", "testLocation");
        metadata.put("partners", partner);

        String testString = """
                [
                  {
                    "name": "test1",
                    "url": "https:/test.com/uid2/optout",
                    "method": "GET",
                    "query_params": [
                      "action=dooptout",
                      "uid2=${ADVERTISING_ID}",
                      "timestamp=${OPTOUT_EPOCH}"
                    ],
                    "additional_headers": [
                      "Authorization: Bearer some_bearer"
                    ],
                    "retry_count": 600,
                    "retry_backoff_ms": 6000
                  },
                  {
                    "name": "test2",
                    "url": "https:/example.com/optout",
                    "method": "POST",
                    "query_params": [
                      "token=${ADVERTISING_ID}",
                      "timestamp=${OPTOUT_EPOCH}"
                    ],
                    "additional_headers": [
                      "Authorization: Bearer bearer2"
                    ],
                    "retry_count": 60,
                    "retry_backoff_ms": 1000
                  }
                ]
                """;

        when(contentStorage.download(any())).thenReturn(new ByteArrayInputStream(testString.getBytes()));

        long endpoints = partnerConfigMonitorV2.loadContent(metadata);

        //Two endpoints senders should be deployed
        assertEquals(2, endpoints);


        testContext.completeNow();
    }
}
