package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.optout.OptOutCloudSync;
import com.uid2.shared.vertx.VertxUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.*;
import org.junit.runner.RunWith;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.InputStream;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Integration tests for OptOutSqsLogProducer deltaproduce endpoint.
 * Requires Java 21 for Mockito compatibility with AWS SDK.
 */
@RunWith(VertxUnitRunner.class)
public class OptOutSqsLogProducerTest {
    
    private Vertx vertx;
    private OptOutSqsLogProducer producer;
    
    private SqsClient sqsClient;
    private ICloudStorage cloudStorage;
    private ICloudStorage cloudStorageDroppedRequests;
    private OptOutCloudSync cloudSync;
    
    private static final String TEST_BUCKET_DROPPED_REQUESTS = "test-bucket-dropped-requests";
    private static final String TEST_QUEUE_URL = "https://sqs.test.amazonaws.com/123456789/test";
    private static final String TEST_API_KEY = "test-api-key";
    private static final String VALID_HASH_BASE64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    private static final String VALID_ID_BASE64 = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
    private static final String TRAFFIC_FILTER_CONFIG_PATH = "./traffic-filter.json";
    private static final String TRAFFIC_CALC_CONFIG_PATH = "./traffic-calc.json";
    
    @Before
    public void setup(TestContext context) throws Exception {
        vertx = Vertx.vertx();
        
        // Create mocks
        sqsClient = mock(SqsClient.class);
        cloudStorage = mock(ICloudStorage.class);
        cloudSync = mock(OptOutCloudSync.class);
        
        JsonObject config = VertxUtils.getJsonConfig(vertx);
        config.put(Const.Config.OptOutSqsQueueUrlProp, TEST_QUEUE_URL)
              .put(Const.Config.OptOutSqsVisibilityTimeoutProp, 240)
              .put(Const.Config.OptOutProducerBufferSizeProp, 65536)
              .put(Const.Config.OptOutProducerReplicaIdProp, 1)
              .put(Const.Config.OptOutInternalApiTokenProp, TEST_API_KEY)
              .put(Const.Config.TrafficFilterConfigPathProp, TRAFFIC_FILTER_CONFIG_PATH)
              .put(Const.Config.TrafficCalcConfigPathProp, TRAFFIC_CALC_CONFIG_PATH)
              .put(Const.Config.OptOutS3BucketDroppedRequestsProp, TEST_BUCKET_DROPPED_REQUESTS);
        
        // Mock cloud sync to return proper S3 paths
        when(cloudSync.toCloudPath(anyString()))
                .thenAnswer(inv -> "sqs-delta/delta/" + inv.getArgument(0));
        
        // Mock S3 upload to succeed by default
        doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());
        
        
        try {
            String traficFilterConfig = """
                    {
                        "blacklist_requests": [

                        ]
                    }
                    """;
            createTrafficConfigFile(traficFilterConfig);
            
            String trafficCalcConfig = """
                    {
                        "traffic_calc_whitelist_ranges": []
                    }
                    """;
            createTrafficCalcConfigFile(trafficCalcConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        // Create producer with mock SqsClient
        producer = new OptOutSqsLogProducer(config, cloudStorage, cloudStorageDroppedRequests, cloudSync, Const.Event.DeltaProduce, sqsClient);
        
        // Deploy verticle
        Async async = context.async();
        vertx.deployVerticle(producer, context.asyncAssertSuccess(v -> async.complete()));
    }
    
    @After
    public void tearDown(TestContext context) {
        if (vertx != null) {
            vertx.close(context.asyncAssertSuccess());
        }
        if (Files.exists(Path.of(TRAFFIC_FILTER_CONFIG_PATH))) {
            try {
                Files.delete(Path.of(TRAFFIC_FILTER_CONFIG_PATH));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (Files.exists(Path.of(TRAFFIC_CALC_CONFIG_PATH))) {
            try {
                Files.delete(Path.of(TRAFFIC_CALC_CONFIG_PATH));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void createTrafficConfigFile(String content) {
        try {
            Path configPath = Path.of(TRAFFIC_FILTER_CONFIG_PATH);
            Files.writeString(configPath, content);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createTrafficCalcConfigFile(String content) {
        try {
            Path configPath = Path.of(TRAFFIC_CALC_CONFIG_PATH);
            Files.writeString(configPath, content);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private Message createMessage(String hash, String id, long timestampMs) {
        JsonObject body = new JsonObject()
                .put("identity_hash", hash)
                .put("advertising_id", id);
        
        Map<MessageSystemAttributeName, String> attrs = new HashMap<>();
        attrs.put(MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(timestampMs));
        
        return Message.builder()
                .body(body.encode())
                .attributes(attrs)
                .messageId("msg-" + UUID.randomUUID())
                .receiptHandle("receipt-" + UUID.randomUUID())
                .build();
    }
    
    @Test
    public void testDeltaProduceEndpoint_successWithMessages(TestContext context) throws Exception {
        Async async = context.async();
        
        // Create messages old enough to process (> 5 minutes)
        long oldTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime),
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime + 1000)
        );
        
        // Mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());
        
        // Mock S3 upload
        doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());
        
        // Call endpoint via HTTP
        int port = Const.Port.ServicePortForOptOut + 1;
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1", 
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("success", response.getString("status"));
                    context.assertTrue(response.getInteger("deltas_produced") >= 1);
                    context.assertTrue(response.getInteger("entries_processed") >= 2);
                    
                    // Verify S3 was called
                    try {
                        verify(cloudStorage, atLeastOnce()).upload(any(InputStream.class), anyString());
                    } catch (Exception e) {
                        context.fail(e);
                    }
                    
                    // Verify SQS delete was called
                    verify(sqsClient, atLeastOnce()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
                    
                    async.complete();
                }));
    }
    
    @Test
    public void testDeltaProduceEndpoint_noMessages(TestContext context) {
        Async async = context.async();
        
        // Mock empty queue
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        
        int port = Const.Port.ServicePortForOptOut + 1;
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("skipped", response.getString("status"));
                    context.assertEquals(0, response.getInteger("deltas_produced"));
                    context.assertEquals(0, response.getInteger("entries_processed"));
                    
                    // Verify no operations
                    try {
                        verify(cloudStorage, never()).upload(any(InputStream.class), anyString());
                    } catch (Exception e) {
                        context.fail(e);
                    }
                    verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
                    
                    async.complete();
                }));
    }
    
    @Test
    public void testDeltaProduceEndpoint_allMessagesTooRecent(TestContext context) {
        Async async = context.async();
        
        // Create recent messages (< 5 minutes old)
        long recentTime = System.currentTimeMillis() - 60_000; // 1 minute ago
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, recentTime)
        );
        
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        
        int port = Const.Port.ServicePortForOptOut + 1;
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("skipped", response.getString("status"));
                    context.assertEquals("All messages are too recent", response.getString("reason"));
                    
                    // No processing should occur
                    try {
                        verify(cloudStorage, never()).upload(any(InputStream.class), anyString());
                    } catch (Exception e) {
                        context.fail(e);
                    }
                    verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
                    
                    async.complete();
                }));
    }
    
    @Test
    public void testDeltaProduceEndpoint_unauthorized(TestContext context) {
        Async async = context.async();
        
        int port = Const.Port.ServicePortForOptOut + 1;
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req.send()) // No auth header
                .compose(resp -> {
                    context.assertEquals(401, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    // Should not call SQS when unauthorized
                    verify(sqsClient, never()).receiveMessage(any(ReceiveMessageRequest.class));
                    async.complete();
                }));
    }
}

