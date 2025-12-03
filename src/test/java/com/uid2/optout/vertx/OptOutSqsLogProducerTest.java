package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.optout.OptOutCloudSync;
import com.uid2.shared.vertx.VertxUtils;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.optout.OptOutCollection;
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
import java.io.ByteArrayInputStream;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doAnswer;
import org.mockito.ArgumentCaptor;

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
    
    private static final String TEST_QUEUE_URL = "https://sqs.test.amazonaws.com/123456789/test";
    private static final String TEST_API_KEY = "test-api-key";
    private static final String VALID_HASH_BASE64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    private static final String VALID_ID_BASE64 = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
    private static final String TRAFFIC_FILTER_CONFIG_PATH = "./traffic-filter.json";
    private static final String TRAFFIC_CALC_CONFIG_PATH = "./traffic-calc.json";
    private static final String MANUAL_OVERRIDE_S3_PATH = "manual-override.json";
    private static final String S3_DELTA_PREFIX = "sqs-delta";
    private static final String TEST_BUCKET_DROPPED_REQUESTS = "test-bucket-dropped-requests";
    
    @Before
    public void setup(TestContext context) throws Exception {
        vertx = Vertx.vertx();
        
        // Create mocks
        sqsClient = mock(SqsClient.class);
        cloudStorage = mock(ICloudStorage.class);
        cloudStorageDroppedRequests = mock(ICloudStorage.class);
        cloudSync = mock(OptOutCloudSync.class);
        
        JsonObject config = VertxUtils.getJsonConfig(vertx);
        config.put(Const.Config.OptOutSqsQueueUrlProp, TEST_QUEUE_URL)
              .put(Const.Config.OptOutSqsVisibilityTimeoutProp, 240)
              .put(Const.Config.OptOutProducerBufferSizeProp, 65536)
              .put(Const.Config.OptOutProducerReplicaIdProp, 1)
              .put(Const.Config.OptOutInternalApiTokenProp, TEST_API_KEY)
              .put(Const.Config.OptOutInternalApiTokenProp, TEST_API_KEY)
              .put(Const.Config.TrafficFilterConfigPathProp, TRAFFIC_FILTER_CONFIG_PATH)
              .put(Const.Config.TrafficCalcConfigPathProp, TRAFFIC_CALC_CONFIG_PATH)
              .put(Const.Config.ManualOverrideS3PathProp, MANUAL_OVERRIDE_S3_PATH)
              .put(Const.Config.OptOutS3BucketDroppedRequestsProp, TEST_BUCKET_DROPPED_REQUESTS)
              .put(Const.Config.OptOutSqsS3FolderProp, S3_DELTA_PREFIX);
        
        // Mock cloud sync to return proper S3 paths
        when(cloudSync.toCloudPath(anyString()))
                .thenAnswer(inv -> "sqs-delta/delta/" + inv.getArgument(0));
        
        // Mock S3 upload to succeed by default
        doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());
        doAnswer(inv -> null).when(cloudStorageDroppedRequests).upload(any(InputStream.class), anyString());

        // Mock getQueueAttributes by default (returns zero messages)
        Map<QueueAttributeName, String> defaultQueueAttrs = new HashMap<>();
        defaultQueueAttrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0");
        defaultQueueAttrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0");
        defaultQueueAttrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED, "0");
        doReturn(GetQueueAttributesResponse.builder()
                        .attributes(defaultQueueAttrs)
                        .build())
                .when(sqsClient).getQueueAttributes(any(GetQueueAttributesRequest.class));

        // Don't mock download with anyString() - let tests mock specific paths as needed
        // Unmocked downloads will return null by default


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
                        "traffic_calc_evaluation_window_seconds": 86400,
                        "traffic_calc_baseline_traffic": 100,
                        "traffic_calc_threshold_multiplier": 5,
                        "traffic_calc_allowlist_ranges": []
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
        return createMessage(hash, id, timestampMs, null, null, null, null);
    }
    
    private Message createMessage(String hash, String id, long timestampMs, String email, String phone, String clientIp, String traceId) {
        JsonObject body = new JsonObject()
                .put("identity_hash", hash)
                .put("advertising_id", id);

        if (email != null) body.put("email", email);
        if (phone != null) body.put("phone", phone);
        if (clientIp != null) body.put("client_ip", clientIp);
        if (traceId != null) body.put("trace_id", traceId);
        
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
        
        int port = Const.Port.ServicePortForOptOut + 1;
        
        // Step 1: Start the job (POST) - returns 202 immediately
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1", 
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    
                    // Step 2: Poll for job completion
                    return pollForCompletion(context, port, 100, 50); // Poll every 100ms, max 50 times (5 seconds)
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertNotNull(result);
                    context.assertEquals("success", result.getString("status"));
                    context.assertTrue(result.getInteger("deltas_produced") >= 1);
                    context.assertTrue(result.getInteger("entries_processed") >= 2);
                    
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
    
    /**
     * Helper method to poll for job completion
     */
    private io.vertx.core.Future<JsonObject> pollForCompletion(TestContext context, int port, long intervalMs, int maxAttempts) {
        return pollForCompletionRecursive(context, port, intervalMs, maxAttempts, 0);
    }
    
    private io.vertx.core.Future<JsonObject> pollForCompletionRecursive(TestContext context, int port, long intervalMs, int maxAttempts, int attempt) {
        if (attempt >= maxAttempts) {
            return io.vertx.core.Future.failedFuture("Job did not complete within timeout");
        }
        
        return vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.GET, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString() + "/status")
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    if (resp.statusCode() != 200) {
                        return io.vertx.core.Future.failedFuture("Status check failed: " + resp.statusCode());
                    }
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject status = new JsonObject(body.toString());
                    String state = status.getString("state");
                    
                    if ("completed".equals(state) || "failed".equals(state)) {
                        return io.vertx.core.Future.succeededFuture(status);
                    }
                    
                    // Still running or idle, wait and poll again
                    io.vertx.core.Promise<JsonObject> promise = io.vertx.core.Promise.promise();
                    vertx.setTimer(intervalMs, id -> {
                        pollForCompletionRecursive(context, port, intervalMs, maxAttempts, attempt + 1)
                                .onComplete(promise);
                    });
                    return promise.future();
                });
    }
    
    @Test
    public void testDeltaProduceEndpoint_noMessages(TestContext context) {
        Async async = context.async();
        
        // Mock empty queue
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        
        int port = Const.Port.ServicePortForOptOut + 1;
        
        // Start job
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    
                    // Poll for completion
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertNotNull(result);
                    context.assertEquals("success", result.getString("status"));
                    context.assertEquals(0, result.getInteger("deltas_produced"));
                    context.assertEquals(0, result.getInteger("entries_processed"));
                    
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
        
        // Start job
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    
                    // Poll for completion
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertNotNull(result);
                    context.assertEquals("skipped", result.getString("status"));
                    context.assertEquals("No deltas produced", result.getString("reason"));
                    
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
    
    @Test
    public void testDeltaProduceEndpoint_concurrentJobPrevention(TestContext context) throws Exception {
        Async async = context.async();
        
        // Create messages that will take some time to process
        long oldTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime)
        );
        
        // Use CountDownLatch to control when the mock returns - ensures job stays running
        java.util.concurrent.CountDownLatch processingLatch = new java.util.concurrent.CountDownLatch(1);
        
        // Mock SQS to wait on latch before returning - keeps job in RUNNING state
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(inv -> {
                    processingLatch.await(); // Wait until test releases
                    return ReceiveMessageResponse.builder().messages(messages).build();
                })
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());
        
        doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());
        
        int port = Const.Port.ServicePortForOptOut + 1;
        
        // Start first job
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    
                    // Immediately try to start a second job (should be rejected)
                    return vertx.createHttpClient()
                            .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                                    Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                            .compose(req -> req
                                    .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                                    .send());
                })
                .compose(resp -> {
                    context.assertEquals(409, resp.statusCode()); // Conflict - job already running
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("conflict", response.getString("status"));
                    context.assertTrue(response.getString("reason").contains("already running"));
                    
                    // Release the latch so first job can complete
                    processingLatch.countDown();
                    async.complete();
                }));
    }
    
    @Test
    public void testDeltaProduceEndpoint_autoClearCompletedJob(TestContext context) throws Exception {
        Async async = context.async();
        
        // Create messages for first job
        long oldTime1 = System.currentTimeMillis() - 400_000;
        List<Message> messages1 = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime1)
        );
        
        // Create messages for second job
        long oldTime2 = System.currentTimeMillis() - 400_000;
        List<Message> messages2 = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime2)
        );
        
        // Mock SQS to return different messages for each job
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                // First job
                .thenReturn(ReceiveMessageResponse.builder().messages(messages1).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build())
                // Second job
                .thenReturn(ReceiveMessageResponse.builder().messages(messages2).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());
        
        doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());
        
        int port = Const.Port.ServicePortForOptOut + 1;
        
        // Start first job
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    
                    // Wait for first job to complete
                    return pollForCompletion(context, port, 100, 50);
                })
                .compose(firstJobStatus -> {
                    context.assertEquals("completed", firstJobStatus.getString("state"));
                    
                    // Now start a second job - should auto-clear the completed first job
                    return vertx.createHttpClient()
                            .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                                    Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                            .compose(req -> req
                                    .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                                    .send());
                })
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode()); // Should succeed (auto-cleared)
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    
                    // Wait for second job to complete
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(secondJobStatus -> {
                    context.assertEquals("completed", secondJobStatus.getString("state"));
                    
                    // Verify both jobs processed messages
                    verify(sqsClient, atLeast(2)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
                    
                    async.complete();
                }));
    }

    @Test
    public void testTrafficFilter_blacklistedMessagesAreDropped(TestContext context) throws Exception {
        Async async = context.async();

        // Setup - update traffic filter config to blacklist specific IP and time range
        long baseTime = System.currentTimeMillis() / 1000 - 400; // 400 seconds ago
        String filterConfig = String.format("""
                {
                    "blacklist_requests": [
                        {
                            "range": [%d, %d],
                            "IPs": ["192.168.1.100"]
                        }
                    ]
                }
                """, baseTime - 100, baseTime + 100);
        createTrafficConfigFile(filterConfig);

        // Setup - create messages: some blacklisted, some not
        long blacklistedTime = (baseTime) * 1000; // Within blacklist range
        long normalTime = (baseTime - 200) * 1000; // Outside blacklist range
        List<Message> messages = Arrays.asList(
                // These should be dropped (blacklisted)
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, blacklistedTime, null, null, "192.168.1.100", null),
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, blacklistedTime + 1000, null, null, "192.168.1.100", null),
                // These should be processed normally
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, normalTime, null, null, "10.0.0.1", null),
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, normalTime + 1000, null, null, "10.0.0.2", null)
        );

        // Setup - mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        int port = Const.Port.ServicePortForOptOut + 1;
        // Act & Assert - call endpoint via HTTP
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals("success", result.getString("status"));

                    // Should process 2 normal entries
                    context.assertEquals(2, result.getInteger("entries_processed"));

                    // Should have 2 dropped requests
                    context.assertEquals(2, result.getInteger("dropped_requests_processed"));

                    // Verify both delta and dropped request files were uploaded
                    try {
                        verify(cloudStorage, atLeastOnce()).upload(any(InputStream.class), anyString());
                        verify(cloudStorageDroppedRequests, atLeastOnce()).upload(any(InputStream.class), anyString());
                    } catch (Exception e) {
                        context.fail(e);
                    }

                    async.complete();
                }));
    }

    @Test
    public void testTrafficFilter_noBlacklistedMessages(TestContext context) throws Exception {
        Async async = context.async();

        // Setup - traffic filter with a blacklisted IP
        long baseTime = System.currentTimeMillis() / 1000 - 400;
        String filterConfig = String.format("""
                {
                    "blacklist_requests": [
                        {
                            "range": [%d, %d],
                            "IPs": ["192.168.1.100"]
                        }
                    ]
                }
                """, baseTime - 100, baseTime + 100);
        createTrafficConfigFile(filterConfig);

        // Setup - create messages that don't match blacklist
        long normalTime = (baseTime - 200) * 1000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, normalTime, null, null, "10.0.0.1", null),
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, normalTime + 1000, null, null, "10.0.0.2", null)
        );

        // Setup - mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        int port = Const.Port.ServicePortForOptOut + 1;
        // Act & Assert - call endpoint via HTTP
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals("success", result.getString("status"));
                    context.assertEquals(2, result.getInteger("entries_processed"));
                    context.assertEquals(0, result.getInteger("dropped_requests_processed"));

                    // Should not upload dropped request file
                    try {
                        verify(cloudStorageDroppedRequests, never()).upload(any(InputStream.class), anyString());
                    } catch (Exception e) {
                        context.fail(e);
                    }

                    async.complete();
                }));
    }

    @Test
    public void testTrafficFilter_allMessagesBlacklisted(TestContext context) throws Exception {
        Async async = context.async();

        // Setup - traffic filter with a blacklisted IP
        long baseTime = System.currentTimeMillis() / 1000 - 400;
        String filterConfig = String.format("""
                {
                    "blacklist_requests": [
                        {
                            "range": [%d, %d],
                            "IPs": ["192.168.1.100"]
                        }
                    ]
                }
                """, baseTime - 100, baseTime + 100);
        createTrafficConfigFile(filterConfig);

        // Setup - create messages that are blacklisted
        long blacklistedTime = baseTime * 1000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, blacklistedTime, null, null, "192.168.1.100", null),
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, blacklistedTime + 1000, null, null, "192.168.1.100", null)
        );

        // Setup - mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build()); 
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        int port = Const.Port.ServicePortForOptOut + 1;
        // Act & Assert - call endpoint via HTTP
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals("success", result.getString("status"));

                    // No entries processed (all blacklisted)
                    context.assertEquals(0, result.getInteger("entries_processed"));

                    // All messages dropped
                    context.assertEquals(2, result.getInteger("dropped_requests_processed"));

                    // Should upload dropped request file but not delta file
                    try {
                        verify(cloudStorageDroppedRequests, atLeastOnce()).upload(any(InputStream.class), anyString());
                        verify(cloudStorage, never()).upload(any(InputStream.class), anyString());
                    } catch (Exception e) {
                        context.fail(e);
                    }

                    async.complete();
                }));
    }

    @Test
    public void testTrafficFilter_messagesWithoutClientIp(TestContext context) throws Exception {
        Async async = context.async();

        // Setup - traffic filter with a blacklisted IP
        long baseTime = System.currentTimeMillis() / 1000 - 400;
        String filterConfig = String.format("""
                {
                    "blacklist_requests": [
                        {
                            "range": [%d, %d],
                            "IPs": ["192.168.1.100"]
                        }
                    ]
                }
                """, baseTime - 100, baseTime + 100);
        createTrafficConfigFile(filterConfig);

        // Create messages without client IP (should not be blacklisted)
        long time = baseTime * 1000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, time, null, null, null, null)
        );

        // Setup - mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        int port = Const.Port.ServicePortForOptOut + 1;
        // Act & Assert - call endpoint via HTTP
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals("success", result.getString("status"));

                    // Message should be processed (not blacklisted due to missing IP)
                    context.assertEquals(1, result.getInteger("entries_processed"));
                    context.assertEquals(0, result.getInteger("dropped_requests_processed"));

                    async.complete();
                }));
    }

    @Test
    public void testTrafficFilterConfig_reloadOnEachBatch(TestContext context) throws Exception {
        Async async = context.async();

        // Setup - initial config with no blacklist
        String initialConfig = """
                {
                    "blacklist_requests": []
                }
                """;
        createTrafficConfigFile(initialConfig);

        // Setup - create messages
        long oldTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime, null, null, "192.168.1.100", null)
        );

        // Setup - mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());    
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        // Act & Assert - first request - should process normally
        int port = Const.Port.ServicePortForOptOut + 1;
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals(1, result.getInteger("entries_processed"));
                    context.assertEquals(0, result.getInteger("dropped_requests_processed"));

                    // Update config to blacklist the IP
                    try {
                        long baseTime = System.currentTimeMillis() / 1000 - 400;
                        String updatedConfig = String.format("""
                                {
                                    "blacklist_requests": [
                                        {
                                            "range": [%d, %d],
                                            "IPs": ["192.168.1.100"]
                                        }
                                    ]
                                }
                                """, baseTime - 100, baseTime + 100);
                        createTrafficConfigFile(updatedConfig);

                        // Reset mocks for second request
                        reset(sqsClient, cloudStorage, cloudStorageDroppedRequests);

                        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());

                        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                                .thenReturn(DeleteMessageBatchResponse.builder().build());

                        doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());
                        doAnswer(inv -> null).when(cloudStorageDroppedRequests).upload(any(InputStream.class), anyString());

                        // Act & Assert - second request - should now be blacklisted
                        vertx.createHttpClient()
                                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                                .compose(req -> req
                                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                                        .send())
                                .compose(resp -> {
                                    context.assertEquals(202, resp.statusCode());
                                    return resp.body();
                                })
                                .compose(body2 -> {
                                    JsonObject response2 = new JsonObject(body2.toString());
                                    context.assertEquals("accepted", response2.getString("status"));
                                    return pollForCompletion(context, port, 100, 50);
                                })
                                .onComplete(context.asyncAssertSuccess(finalStatus2 -> {
                                    context.assertEquals("completed", finalStatus2.getString("state"));
                                    JsonObject result2 = finalStatus2.getJsonObject("result");
                                    // Now should be blacklisted
                                    context.assertEquals(0, result2.getInteger("entries_processed"));
                                    context.assertEquals(1, result2.getInteger("dropped_requests_processed"));
                                    async.complete();
                                }));
                    } catch (Exception e) {
                        context.fail(e);
                    }
                }));
    }

    @Test
    public void testTrafficCalculator_defaultStatus(TestContext context) throws Exception {
        Async async = context.async();

        // Setup - traffic calc config with required fields
        String trafficCalcConfig = """
                {
                    "traffic_calc_evaluation_window_seconds": 86400,
                    "traffic_calc_baseline_traffic": 100,
                    "traffic_calc_threshold_multiplier": 5,
                    "traffic_calc_allowlist_ranges": []
                }
                """;
        createTrafficCalcConfigFile(trafficCalcConfig);

        // Setup - no manual override
        when(cloudStorage.download(anyString()))
                .thenReturn(null);

        // Setup - create messages that will result in DEFAULT status
        long oldTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime, null, null, "10.0.0.1", null),
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime + 1000, null, null, "10.0.0.2", null)
        );

        // Setup - mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        int port = Const.Port.ServicePortForOptOut + 1;

        // Act & Assert - call endpoint via HTTP
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals("success", result.getString("status"));

                    // Should process messages normally (DEFAULT status)
                    context.assertEquals(2, result.getInteger("entries_processed"));
                    context.assertTrue(result.getInteger("deltas_produced") >= 1);

                    // Verify upload happened
                    try {
                        verify(cloudStorage, atLeastOnce()).upload(any(InputStream.class), anyString());
                    } catch (Exception e) {
                        context.fail(e);
                    }

                    async.complete();
                }));
    }

    @Test
    public void testManualOverride_delayedProcessing(TestContext context) throws Exception {
        Async async = context.async();

        // Setup - mock manual override set to DELAYED_PROCESSING
        JsonObject manualOverride = new JsonObject().put("manual_override", "DELAYED_PROCESSING");
        doReturn(new java.io.ByteArrayInputStream(manualOverride.encode().getBytes()))
                .when(cloudStorage).download(MANUAL_OVERRIDE_S3_PATH); // At root of bucket

        // Setup - create messages (won't be processed due to override)
        long oldTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime, null, null, "10.0.0.1", null),
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime + 1000, null, null, "10.0.0.2", null)
        );

        List<Message> allMessages = new ArrayList<>(messages);

        // Setup - mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(allMessages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        int port = Const.Port.ServicePortForOptOut + 1;
        // Act & Assert - call endpoint via HTTP
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals("success", result.getString("status"));

                    // Should not process anything - manual override checked at start
                    context.assertEquals(0, result.getInteger("entries_processed"));
                    context.assertEquals(0, result.getInteger("deltas_produced"));

                    // No SQS deletions should occur (messages not processed)
                    verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));

                    async.complete();
                }));
    }

    @Test
    public void testManualOverride_default_bypassesTrafficCalculation(TestContext context) throws Exception {
        Async async = context.async();

        // Setup - setup time: current time
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;

        // Create delta files with timestamps distributed over 48 hours
        List<Long> timestamps = new ArrayList<>();

        // Past window: t-47h to t-25h (add 10 entries)
        for (int i = 0; i < 10; i++) {
            timestamps.add(t - 47*3600 + i * 1000);
        }

        // Current window: t-23h to t-1h (add 100 entries - 10x past)
        for (int i = 0; i < 100; i++) {
            timestamps.add(t - 23*3600 + i * 1000);
        }

        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);

        // Setup - mock manual override set to DEFAULT
        JsonObject manualOverride = new JsonObject().put("manual_override", "DEFAULT");

        // Mock S3 operations for this test
        // Use doAnswer to create fresh streams on each call (streams are consumed on read)
        doReturn(Arrays.asList("sqs-delta/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat"))
                .when(cloudStorage).list("sqs-delta");
        doAnswer(inv -> new ByteArrayInputStream(deltaFileBytes))
                .when(cloudStorage).download("sqs-delta/delta/optout-delta--01_2025-11-13T00.00.00Z_aaaaaaaa.dat");
        doAnswer(inv -> new java.io.ByteArrayInputStream(manualOverride.encode().getBytes()))
                .when(cloudStorage).download("manual-override.json");

        // Setup - SQS messages, 10 messages in same window
        long oldTime = (t - 600) * 1000;
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime - (i * 1000), null, null, "10.0.0." + i, null));
        }

        // Setup - mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        int port = Const.Port.ServicePortForOptOut + 1;
        // Act & Assert - call endpoint via HTTP
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals("success", result.getString("status"));

                    // Should process 10 messages and produce 1 delta (all in same window)
                    context.assertEquals(10, result.getInteger("entries_processed"));
                    context.assertEquals(1, result.getInteger("deltas_produced"));

                    verify(sqsClient, atLeastOnce()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));

                    async.complete();
                }));
    }

    @Test
    public void testTrafficCalculator_detectsSpikeInCurrentWindow(TestContext context) throws Exception {
        Async async = context.async();

        // Threshold = baseline * multiplier = 100 * 5 = 500
        // We have 610 messages, which exceeds 500, so spike should be detected
        String trafficCalcConfig = """
                {
                    "traffic_calc_evaluation_window_seconds": 86400,
                    "traffic_calc_baseline_traffic": 100,
                    "traffic_calc_threshold_multiplier": 5,
                    "traffic_calc_allowlist_ranges": []
                }
                """;
        createTrafficCalcConfigFile(trafficCalcConfig);

        // Setup time
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;

        // Create historical delta files showing low baseline traffic (2 records from 24-48h ago)
        List<Long> timestamps = new ArrayList<>();
        timestamps.add(t - 36*3600); // 36 hours ago
        timestamps.add(t - 36*3600 + 1000);
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);

        // Reset cloudStorage mock to ensure clean state
        reset(cloudStorage);

        // Re-mock S3 upload (needed after reset)
        doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());

        // Mock S3 operations for historical data
        // Use doAnswer to create fresh streams on each call
        doReturn(Arrays.asList("sqs-delta/delta/optout-delta--01_2025-11-13T00.00.00Z_baseline.dat"))
                .when(cloudStorage).list("sqs-delta");
        doAnswer(inv -> new ByteArrayInputStream(deltaFileBytes))
                .when(cloudStorage).download("sqs-delta/delta/optout-delta--01_2025-11-13T00.00.00Z_baseline.dat");

        // No manual override set (returns null)
        doReturn(null).when(cloudStorage).download("manual-override.json");

        // Setup SQS messages
        long baseTime = (t - 600) * 1000;
        
        List<Message> allMessages = new ArrayList<>();
        // Create 610 messages with timestamps spread over ~4 minutes (within the 5-minute window)
        for (int i = 0; i < 610; i++) {
            // Timestamps range from (t-600) to (t-600-240) seconds = t-600 to t-840
            // All within a single 5-minute window for traffic calculation, and all > 5 min old
            long timestampMs = baseTime - (i * 400); // ~400ms apart going backwards, total span ~244 seconds
            allMessages.add(createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 
                    timestampMs, null, null, "10.0.0." + (i % 256), null));
        }

        // Mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(allMessages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());
        
        // Mock getQueueAttributes to return zero invisible messages (doesn't affect the spike detection)
        Map<QueueAttributeName, String> queueAttrs = new HashMap<>();
        queueAttrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0");
        queueAttrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0");
        queueAttrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED, "0");
        doReturn(GetQueueAttributesResponse.builder()
                        .attributes(queueAttrs)
                        .build())
                .when(sqsClient).getQueueAttributes(any(GetQueueAttributesRequest.class));

        int port = Const.Port.ServicePortForOptOut + 1;

        // Act & Assert
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals("skipped", result.getString("status"));

                    // Expected behavior:
                    // All 610 messages are within a single 5-minute window
                    // Traffic calculator counts them all and detects spike (>=500 threshold)
                    // DELAYED_PROCESSING is triggered, no delta uploaded
                    // The entries_processed count reflects how many were read before spike detection
                    context.assertTrue(result.getInteger("entries_processed") <= 610);
                    context.assertEquals(0, result.getInteger("deltas_produced"));

                    // Verify manual override was set to DELAYED_PROCESSING on S3
                    try {
                        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
                        ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
                        verify(cloudStorage, atLeastOnce()).upload(streamCaptor.capture(), pathCaptor.capture());

                        // Check if manual-override.json was uploaded
                        boolean overrideSet = false;
                        for (int i = 0; i < pathCaptor.getAllValues().size(); i++) {
                            if (pathCaptor.getAllValues().get(i).equals("manual-override.json")) {
                                overrideSet = true;
                                break;
                            }
                        }
                        context.assertTrue(overrideSet, "Manual override should be set to DELAYED_PROCESSING after detecting spike");
                    } catch (Exception e) {
                        context.fail(e);
                    }

                    async.complete();
                }));
    }

    @Test
    public void testCircuitBreaker_stopsProcessingWhenMessageLimitExceeded(TestContext context) throws Exception {
        Async async = context.async();

        // Use low threshold (100) so circuit breaker triggers before traffic calculator
        String trafficCalcConfig = """
                {
                    "traffic_calc_evaluation_window_seconds": 86400,
                    "traffic_calc_baseline_traffic": 20,
                    "traffic_calc_threshold_multiplier": 5,
                    "traffic_calc_allowlist_ranges": []
                }
                """;
        createTrafficCalcConfigFile(trafficCalcConfig);

        // Reset cloudStorage mock to ensure clean state
        reset(cloudStorage);

        // Re-mock S3 upload (needed after reset)
        doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());

        // No manual override set (returns null)
        doReturn(null).when(cloudStorage).download("manual-override.json");

        // Setup time
        long currentTime = System.currentTimeMillis() / 1000;
        long t = currentTime;

        // Create 200 messages - exceeds threshold (20 * 5 = 100)
        long oldTime = (t - 600) * 1000; // 10 minutes ago
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            messages.add(createMessage(VALID_HASH_BASE64, VALID_ID_BASE64,
                    oldTime - (i * 1000), null, null, "10.0.0." + (i % 256), null));
        }

        // Mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        int port = Const.Port.ServicePortForOptOut + 1;

        // Act & Assert
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals("skipped", result.getString("status"));

                    // Expected behavior:
                    // SqsWindowReader hits maxMessagesPerWindow limit (100) during reading
                    // Circuit breaker triggers DELAYED_PROCESSING immediately
                    // Processing stops before any messages are counted as processed
                    context.assertEquals(0, result.getInteger("entries_processed"));
                    context.assertEquals(0, result.getInteger("deltas_produced"));

                    // Verify manual override was set to DELAYED_PROCESSING on S3
                    try {
                        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
                        verify(cloudStorage, atLeastOnce()).upload(any(InputStream.class), pathCaptor.capture());

                        // Check if manual-override.json was uploaded
                        boolean overrideSet = pathCaptor.getAllValues().stream()
                                .anyMatch(path -> path.equals("manual-override.json"));
                        context.assertTrue(overrideSet, "Circuit breaker should set DELAYED_PROCESSING override");
                    } catch (Exception e) {
                        context.fail(e);
                    }

                    // Verify NO messages were deleted from SQS (processing stopped before completion)
                    verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));

                    async.complete();
                }));
    }

    @Test
    public void testManualOverride_notSet(TestContext context) throws Exception {
        Async async = context.async();

        // Setup - mock no manual override file
        when(cloudStorage.download(anyString()))
                .thenReturn(null);

        // Setup - create messages
        long oldTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime, null, null, "10.0.0.1", null)
        );

        // Setup - mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        int port = Const.Port.ServicePortForOptOut + 1;
        // Act & Assert - call endpoint via HTTP
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    context.assertEquals("completed", finalStatus.getString("state"));
                    JsonObject result = finalStatus.getJsonObject("result");
                    context.assertEquals("success", result.getString("status"));

                    // Should process normally with traffic calc (no override)
                    context.assertEquals(1, result.getInteger("entries_processed"));

                    async.complete();
                }));
    }

    @Test
    public void testS3UploadFailure_messagesNotDeletedFromSqs(TestContext context) throws Exception {
        Async async = context.async();

        // Create messages to process
        long oldTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime, null, null, "10.0.0.1", null),
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime + 1000, null, null, "10.0.0.2", null)
        );

        // Mock SQS to return messages
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());

        // Mock S3 upload to FAIL
        doThrow(new RuntimeException("S3 upload failed - simulated error"))
                .when(cloudStorage).upload(any(InputStream.class), anyString());

        int port = Const.Port.ServicePortForOptOut + 1;

        // Start job
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("accepted", response.getString("status"));
                    return pollForCompletion(context, port, 100, 50);
                })
                .onComplete(context.asyncAssertSuccess(finalStatus -> {
                    // Job should fail due to S3 error
                    context.assertEquals("failed", finalStatus.getString("state"));
                    context.assertTrue(finalStatus.getString("error").contains("S3") || 
                                      finalStatus.getString("error").contains("upload"));

                    // CRITICAL: Messages should NOT be deleted from SQS when upload fails
                    verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));

                    async.complete();
                }));
    }

    @Test
    public void testStatusEndpoint_showsRunningJob(TestContext context) throws Exception {
        Async async = context.async();

        // Create messages
        long oldTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime)
        );

        // Use CountDownLatch to keep job running
        java.util.concurrent.CountDownLatch processingLatch = new java.util.concurrent.CountDownLatch(1);

        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(inv -> {
                    processingLatch.await();
                    return ReceiveMessageResponse.builder().messages(messages).build();
                })
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        int port = Const.Port.ServicePortForOptOut + 1;

        // Start job
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> {
                    // Immediately check status - should show "running"
                    return vertx.createHttpClient()
                            .request(io.vertx.core.http.HttpMethod.GET, port, "127.0.0.1",
                                    Endpoints.OPTOUT_DELTA_PRODUCE.toString() + "/status")
                            .compose(req -> req
                                    .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                                    .send());
                })
                .compose(resp -> {
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject status = new JsonObject(body.toString());
                    context.assertEquals("running", status.getString("state"));
                    context.assertNotNull(status.getString("start_time"));

                    // Release latch so job can complete
                    processingLatch.countDown();
                    async.complete();
                }));
    }

    @Test
    public void testStatusEndpoint_showsFailedJob(TestContext context) throws Exception {
        Async async = context.async();

        // Create messages
        long oldTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime)
        );

        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());

        // Make S3 upload fail
        doThrow(new RuntimeException("Simulated S3 failure"))
                .when(cloudStorage).upload(any(InputStream.class), anyString());

        int port = Const.Port.ServicePortForOptOut + 1;

        // Start job and wait for it to fail
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                        .send())
                .compose(resp -> {
                    context.assertEquals(202, resp.statusCode());
                    return resp.body();
                })
                .compose(body -> pollForCompletion(context, port, 100, 50))
                .compose(finalStatus -> {
                    context.assertEquals("failed", finalStatus.getString("state"));

                    // Now call status endpoint directly to verify failed state is persisted
                    return vertx.createHttpClient()
                            .request(io.vertx.core.http.HttpMethod.GET, port, "127.0.0.1",
                                    Endpoints.OPTOUT_DELTA_PRODUCE.toString() + "/status")
                            .compose(req -> req
                                    .putHeader("Authorization", "Bearer " + TEST_API_KEY)
                                    .send());
                })
                .compose(resp -> {
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject status = new JsonObject(body.toString());
                    context.assertEquals("failed", status.getString("state"));
                    context.assertNotNull(status.getString("error"));
                    context.assertNotNull(status.getString("start_time"));
                    context.assertNotNull(status.getString("end_time"));
                    context.assertNotNull(status.getInteger("duration_seconds"));

                    async.complete();
                }));
    }

    @Test
    public void testDeltaProduceEndpoint_invalidApiKey(TestContext context) {
        Async async = context.async();

        int port = Const.Port.ServicePortForOptOut + 1;
        vertx.createHttpClient()
                .request(io.vertx.core.http.HttpMethod.POST, port, "127.0.0.1",
                        Endpoints.OPTOUT_DELTA_PRODUCE.toString())
                .compose(req -> req
                        .putHeader("Authorization", "Bearer wrong-api-key")
                        .send())
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

    /**
     * Create delta file bytes with specified timestamps
     */
    private byte[] createDeltaFileBytes(List<Long> timestamps) throws Exception {
        // Create OptOutEntry objects using newTestEntry
        List<OptOutEntry> entries = new ArrayList<>();

        long idCounter = 1000; // Use incrementing IDs for test entries
        for (long timestamp : timestamps) {
            entries.add(OptOutEntry.newTestEntry(idCounter++, timestamp));
        }

        // Create OptOutCollection
        OptOutCollection collection = new OptOutCollection(entries.toArray(new OptOutEntry[0]));
        return collection.getStore();
    }
}

