package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.shared.cloud.CloudStorageException;
import com.uid2.shared.cloud.ICloudStorage;
import com.uid2.shared.optout.OptOutCloudSync;
import com.uid2.shared.optout.OptOutCollection;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.vertx.VertxUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for OptOutSqsLogProducer deltaproduce endpoint.
 * Requires Java 21 for Mockito compatibility with AWS SDK.
 */
@ExtendWith(VertxExtension.class)
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
    private static final int TEST_PORT = Const.Port.ServicePortForOptOut + 1;
    private static final String DELTA_PRODUCE_ENDPOINT = Endpoints.OPTOUT_DELTA_PRODUCE.toString();
    private static final String STATUS_ENDPOINT = DELTA_PRODUCE_ENDPOINT + "/status";
    
    @BeforeEach
    public void setup(Vertx vertx, VertxTestContext testContext) throws Exception {
        this.vertx = vertx;
        
        // create mocks
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
              .put(Const.Config.TrafficFilterConfigPathProp, TRAFFIC_FILTER_CONFIG_PATH)
              .put(Const.Config.TrafficCalcConfigPathProp, TRAFFIC_CALC_CONFIG_PATH)
              .put(Const.Config.ManualOverrideS3PathProp, MANUAL_OVERRIDE_S3_PATH)
              .put(Const.Config.OptOutS3BucketDroppedRequestsProp, TEST_BUCKET_DROPPED_REQUESTS)
              .put(Const.Config.OptOutSqsS3FolderProp, S3_DELTA_PREFIX)
              .put(Const.Config.OptOutMaxMessagesPerFileProp, 100);
        
        // mock cloud sync to return proper s3 paths
        when(cloudSync.toCloudPath(anyString()))
                .thenAnswer(inv -> "sqs-delta/delta/" + inv.getArgument(0));
        
        // mock s3 upload to succeed by default
        doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());
        doAnswer(inv -> null).when(cloudStorageDroppedRequests).upload(any(InputStream.class), anyString());

        // mock s3 list and download for traffic calculator
        when(cloudStorage.list(anyString())).thenReturn(Arrays.asList("sqs-delta/delta/optout-delta-001_2025-01-01T00.00.00Z_aaaaaaaa.dat"));
        when(cloudStorage.download(MANUAL_OVERRIDE_S3_PATH)).thenThrow(new CloudStorageException("file not found"));
        when(cloudStorage.download(argThat(path -> path != null && path.contains("optout-delta"))))
            .thenAnswer(inv -> new ByteArrayInputStream(createMinimalDeltaFileBytes()));

        // mock getQueueAttributes to return zero messages by default
        Map<QueueAttributeName, String> defaultQueueAttrs = new HashMap<>();
        defaultQueueAttrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0");
        defaultQueueAttrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0");
        defaultQueueAttrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED, "0");
        doReturn(GetQueueAttributesResponse.builder()
                        .attributes(defaultQueueAttrs)
                        .build())
                .when(sqsClient).getQueueAttributes(any(GetQueueAttributesRequest.class));


        // default config files
        try {
            String traficFilterConfig = """
                    {
                        "denylist_requests": [
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
        
        // create producer with mocks
        producer = new OptOutSqsLogProducer(config, cloudStorage, cloudStorageDroppedRequests, cloudSync, Const.Event.DeltaProduce, sqsClient);
        
        // deploy verticle
        vertx.deployVerticle(producer, testContext.succeeding(id -> testContext.completeNow()));
    }
    
    @AfterEach
    public void tearDown() {
        // clean up config files
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

    // ==================== Message Creation Helpers ====================

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

    /** Creates N old messages (> 5 minutes ago) with unique IPs */
    private List<Message> createOldMessages(int count) {
        long baseTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            messages.add(createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, baseTime + (i * 1000), 
                    null, null, "10.0.0." + (i + 1), null));
        }
        return messages;
    }

    /** Creates N old messages with specific client IP */
    private List<Message> createOldMessagesWithIp(int count, String clientIp) {
        long baseTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            messages.add(createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, baseTime + (i * 1000), 
                    null, null, clientIp, null));
        }
        return messages;
    }

    // ==================== SQS Mock Helpers ====================

    /** Mock SQS to return messages once, then empty */
    private void mockSqsToReturnMessages(List<Message> messages) {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
    }

    /** Mock SQS to return empty queue */
    private void mockSqsEmptyQueue() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
    }

    /** Mock SQS delete to succeed */
    private void mockSqsDeleteSuccess() {
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());
    }

    /** Mock SQS getQueueAttributes to return zero messages */
    private void mockSqsQueueAttributes() {
        Map<QueueAttributeName, String> attrs = new HashMap<>();
        attrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0");
        attrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0");
        attrs.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED, "0");
        doReturn(GetQueueAttributesResponse.builder().attributes(attrs).build())
                .when(sqsClient).getQueueAttributes(any(GetQueueAttributesRequest.class));
    }

    // ==================== Cloud Storage Mock Helpers ====================

    /** Setup cloud storage mocks for spike detection test */
    private void setupCloudStorageForSpikeTest(byte[] deltaFileBytes) {
        try {
            doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());
            doReturn(Arrays.asList("sqs-delta/delta/optout-delta--01_2025-11-13T00.00.00Z_baseline.dat"))
                    .when(cloudStorage).list("sqs-delta");
            doAnswer(inv -> new ByteArrayInputStream(deltaFileBytes))
                    .when(cloudStorage).download("sqs-delta/delta/optout-delta--01_2025-11-13T00.00.00Z_baseline.dat");
            doThrow(new CloudStorageException("file not found"))
                    .when(cloudStorage).download("manual-override.json");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Helper to reset mocks between sequential test phases */
    private void resetMocksForSecondRequest(List<Message> messages) {
        reset(sqsClient, cloudStorage, cloudStorageDroppedRequests);
        
        mockSqsToReturnMessages(messages);
        mockSqsDeleteSuccess();
        mockSqsQueueAttributes();
        
        try {
            doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());
            doAnswer(inv -> null).when(cloudStorageDroppedRequests).upload(any(InputStream.class), anyString());
            
            when(cloudStorage.list(anyString()))
                    .thenReturn(Arrays.asList("sqs-delta/delta/optout-delta-001_2025-01-01T00.00.00Z_aaaaaaaa.dat"));
            when(cloudStorage.download(MANUAL_OVERRIDE_S3_PATH))
                    .thenThrow(new CloudStorageException("file not found"));
            when(cloudStorage.download(argThat(path -> path != null && path.contains("optout-delta"))))
                    .thenAnswer(inv -> new ByteArrayInputStream(createMinimalDeltaFileBytes()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ==================== Traffic Config Helpers ====================

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

    /** Create denylist filter config for specific IP and time range */
    private void setupDenylistConfig(String ip) {
        long baseTime = System.currentTimeMillis() / 1000 - 400;
        String filterConfig = String.format("""
                {
                    "denylist_requests": [
                        {
                            "range": [%d, %d],
                            "IPs": ["%s"]
                        }
                    ]
                }
                """, baseTime - 100, baseTime + 100, ip);
        createTrafficConfigFile(filterConfig);
    }

    // ==================== HTTP Request Helpers ====================

    /** Make HTTP request and return response with status code and parsed JSON body */
    private Future<JsonObject> httpRequest(HttpMethod method, String path, String authToken) {
        return vertx.createHttpClient()
                .request(method, TEST_PORT, "127.0.0.1", path)
                .compose(req -> {
                    if (authToken != null) {
                        req.putHeader("Authorization", "Bearer " + authToken);
                    }
                    return req.send();
                })
                .compose(resp -> resp.body().map(body -> {
                    JsonObject result = new JsonObject().put("_statusCode", resp.statusCode());
                    String bodyStr = body.toString();
                    // try to parse as JSON, otherwise store as raw string
                    try {
                        result.put("_body", new JsonObject(bodyStr));
                    } catch (Exception e) {
                        result.put("_bodyRaw", bodyStr);
                    }
                    return result;
                }));
    }

    /** Make authenticated HTTP request */
    private Future<JsonObject> httpRequest(HttpMethod method, String path) {
        return httpRequest(method, path, TEST_API_KEY);
    }

    /** Start a delta production job and poll until completion */
    private Future<JsonObject> startJobAndPollForCompletion() {
        return httpRequest(HttpMethod.POST, DELTA_PRODUCE_ENDPOINT)
                .compose(resp -> {
                    assertEquals(202, resp.getInteger("_statusCode"));
                    assertEquals("accepted", resp.getJsonObject("_body").getString("status"));
                    return pollForCompletion();
                });
    }

    /** POST to start a job, return status code and body */
    private Future<JsonObject> postStartJob() {
        return httpRequest(HttpMethod.POST, DELTA_PRODUCE_ENDPOINT);
    }

    /** POST without authentication */
    private Future<JsonObject> postWithoutAuth() {
        return httpRequest(HttpMethod.POST, DELTA_PRODUCE_ENDPOINT, null);
    }

    /** POST with specific auth token */
    private Future<JsonObject> postWithAuth(String authToken) {
        return httpRequest(HttpMethod.POST, DELTA_PRODUCE_ENDPOINT, authToken);
    }

    /** Get job status */
    private Future<JsonObject> getJobStatus() {
        return httpRequest(HttpMethod.GET, STATUS_ENDPOINT)
                .compose(resp -> {
                    assertEquals(200, resp.getInteger("_statusCode"));
                    return Future.succeededFuture(resp.getJsonObject("_body"));
                });
    }

    // ==================== Polling Helpers ====================
    
    /** Poll for job completion with default settings */
    private Future<JsonObject> pollForCompletion() {
        return pollForCompletion(100, 50);
    }

    /** Poll for job completion with custom settings */
    private Future<JsonObject> pollForCompletion(long intervalMs, int maxAttempts) {
        return pollForCompletionRecursive(intervalMs, maxAttempts, 0);
    }
    
    private Future<JsonObject> pollForCompletionRecursive(long intervalMs, int maxAttempts, int attempt) {
        if (attempt >= maxAttempts) {
            return Future.failedFuture("Job did not complete within timeout");
        }
        
        return httpRequest(HttpMethod.GET, STATUS_ENDPOINT)
                .compose(resp -> {
                    if (resp.getInteger("_statusCode") != 200) {
                        return Future.failedFuture("Status check failed: " + resp.getInteger("_statusCode"));
                    }
                    JsonObject status = resp.getJsonObject("_body");
                    String state = status.getString("state");
                    
                    if ("completed".equals(state) || "failed".equals(state)) {
                        return Future.succeededFuture(status);
                    }
                    
                    // still running or idle, wait and poll again
                    Promise<JsonObject> promise = Promise.promise();
                    vertx.setTimer(intervalMs, id -> 
                        pollForCompletionRecursive(intervalMs, maxAttempts, attempt + 1).onComplete(promise));
                    return promise.future();
                });
    }

    // ==================== Delta File Helpers ====================

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

    /**
     * Create minimal delta file bytes with a single recent timestamp for traffic calculator
     */
    private static byte[] createMinimalDeltaFileBytes() {
        try {
            long timestamp = System.currentTimeMillis() / 1000 - 3600; // 1 hour ago
            OptOutEntry entry = OptOutEntry.newTestEntry(1, timestamp);
            OptOutCollection collection = new OptOutCollection(new OptOutEntry[]{entry});
            return collection.getStore();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create minimal delta file bytes", e);
        }
    }

    // ==================== Assertion Helpers ====================

    /** Extract and return result from completed job status, asserting the job completed successfully */
    private JsonObject assertJobCompletedWithSuccess(JsonObject status) {
        assertEquals("completed", status.getString("state"));
        JsonObject result = status.getJsonObject("result");
        assertNotNull(result);
        assertEquals("success", result.getString("status"));
        return result;
    }

    /** Assert job completed with success and verify minimum counts */
    private void assertJobCompletedWithSuccess(JsonObject status, int minEntries, int minDeltas) {
        JsonObject result = assertJobCompletedWithSuccess(status);
        assertTrue(result.getInteger("entries_processed") >= minEntries,
                "Expected at least " + minEntries + " entries, got " + result.getInteger("entries_processed"));
        assertTrue(result.getInteger("deltas_produced") >= minDeltas,
                "Expected at least " + minDeltas + " deltas, got " + result.getInteger("deltas_produced"));
    }

    /** Assert job completed with skipped status (no stop reason check) */
    private JsonObject assertJobCompletedWithSkipped(JsonObject status) {
        assertEquals("completed", status.getString("state"));
        JsonObject result = status.getJsonObject("result");
        assertNotNull(result);
        assertEquals("skipped", result.getString("status"));
        return result;
    }

    /** Assert job completed with skipped status and specific stop reason */
    private void assertJobCompletedWithSkipped(JsonObject status, String expectedStopReason) {
        JsonObject result = assertJobCompletedWithSkipped(status);
        assertEquals(expectedStopReason, result.getString("stop_reason"));
    }

    /** Assert job completed with halted status and specific stop reason */
    private JsonObject assertJobCompletedWithHalted(JsonObject status, String expectedStopReason) {
        assertEquals("completed", status.getString("state"));
        JsonObject result = status.getJsonObject("result");
        assertNotNull(result);
        assertEquals("halted", result.getString("status"));
        assertEquals(expectedStopReason, result.getString("stop_reason"));
        return result;
    }

    /** Assert job failed */
    private void assertJobFailed(JsonObject status) {
        assertEquals("failed", status.getString("state"));
    }

    /** Verify cloud storage upload was called at least once */
    private void verifyCloudStorageUploaded() {
        try {
            verify(cloudStorage, atLeastOnce()).upload(any(InputStream.class), anyString());
        } catch (Exception e) {
            fail(e);
        }
    }

    /** Verify cloud storage upload was never called */
    private void verifyCloudStorageNotUploaded() {
        try {
            verify(cloudStorage, never()).upload(any(InputStream.class), anyString());
        } catch (Exception e) {
            fail(e);
        }
    }

    /** Verify dropped requests cloud storage upload was called at least once */
    private void verifyDroppedRequestsUploaded() {
        try {
            verify(cloudStorageDroppedRequests, atLeastOnce()).upload(any(InputStream.class), anyString());
        } catch (Exception e) {
            fail(e);
        }
    }

    /** Verify dropped requests cloud storage upload was never called */
    private void verifyDroppedRequestsNotUploaded() {
        try {
            verify(cloudStorageDroppedRequests, never()).upload(any(InputStream.class), anyString());
        } catch (Exception e) {
            fail(e);
        }
    }

    /** Verify SQS messages were deleted */
    private void verifySqsMessagesDeleted() {
        verify(sqsClient, atLeastOnce()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    /** Verify SQS messages were not deleted */
    private void verifySqsMessagesNotDeleted() {
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    /** Verify SQS receive was never called (used for auth failure tests) */
    private void verifySqsReceiveNotCalled() {
        verify(sqsClient, never()).receiveMessage(any(ReceiveMessageRequest.class));
    }

    // ==================== Tests ====================
    
    @Test
    public void testDeltaProduceEndpoint_successWithMessages(VertxTestContext testContext) {
        mockSqsToReturnMessages(createOldMessages(2));
        mockSqsDeleteSuccess();
        
        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    assertJobCompletedWithSuccess(finalStatus, 2, 1);
                    verifyCloudStorageUploaded();
                    verifySqsMessagesDeleted();
                    testContext.completeNow();
                }));
    }
    
    @Test
    public void testDeltaProduceEndpoint_noMessages(VertxTestContext testContext) {
        mockSqsEmptyQueue();
        
        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    JsonObject result = assertJobCompletedWithSkipped(finalStatus);
                    assertEquals(0, result.getInteger("deltas_produced"));
                    assertEquals(0, result.getInteger("entries_processed"));
                    verifyCloudStorageNotUploaded();
                    verifySqsMessagesNotDeleted();
                    testContext.completeNow();
                }));
    }
    
    @Test
    public void testDeltaProduceEndpoint_allMessagesTooRecent(VertxTestContext testContext) {
        // create recent messages (< 5 minutes old)
        long recentTime = System.currentTimeMillis() - 60_000; // 1 minute ago
        List<Message> messages = Arrays.asList(createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, recentTime));
        mockSqsToReturnMessages(messages);
        
        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    assertJobCompletedWithSkipped(finalStatus, "MESSAGES_TOO_RECENT");
                    verifyCloudStorageNotUploaded();
                    verifySqsMessagesNotDeleted();
                    testContext.completeNow();
                }));
    }
    
    @Test
    public void testDeltaProduceEndpoint_unauthorized(VertxTestContext testContext) {
        postWithoutAuth()
                .onComplete(testContext.succeeding(resp -> {
                    assertEquals(401, resp.getInteger("_statusCode"));
                    verifySqsReceiveNotCalled();
                    testContext.completeNow();
                }));
    }
    
    @Test
    public void testDeltaProduceEndpoint_concurrentJobPrevention(VertxTestContext testContext) {
        List<Message> messages = createOldMessages(1);
        CountDownLatch processingLatch = new CountDownLatch(1);
        
        // mock sqs to wait on latch before returning - keeps job running
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(inv -> {
                    processingLatch.await();
                    return ReceiveMessageResponse.builder().messages(messages).build();
                })
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        mockSqsDeleteSuccess();
        
        postStartJob()
                .compose(resp -> {
                    assertEquals(202, resp.getInteger("_statusCode"));
                    assertEquals("accepted", resp.getJsonObject("_body").getString("status"));
                    // immediately try to start a second job
                    return postStartJob();
                })
                .onComplete(testContext.succeeding(resp -> {
                    assertEquals(409, resp.getInteger("_statusCode")); // conflict - job already running
                    JsonObject body = resp.getJsonObject("_body");
                    assertEquals("conflict", body.getString("status"));
                    assertTrue(body.getString("reason").contains("already running"));
                    
                    processingLatch.countDown();
                    testContext.completeNow();
                }));
    }
    
    @Test
    public void testDeltaProduceEndpoint_autoClearCompletedJob(VertxTestContext testContext) {
        List<Message> messages1 = createOldMessages(1);
        List<Message> messages2 = createOldMessages(1);
        
        // mock SQS to return different messages for each job
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages1).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(messages2).build())
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        mockSqsDeleteSuccess();
        
        startJobAndPollForCompletion()
                .compose(firstJobStatus -> {
                    assertEquals("completed", firstJobStatus.getString("state"));
                    // start a second job - should auto-clear the completed first job
                    return postStartJob();
                })
                .compose(resp -> {
                    assertEquals(202, resp.getInteger("_statusCode")); // should succeed (auto-cleared)
                    assertEquals("accepted", resp.getJsonObject("_body").getString("status"));
                    return pollForCompletion();
                })
                .onComplete(testContext.succeeding(secondJobStatus -> {
                    assertEquals("completed", secondJobStatus.getString("state"));
                    verify(sqsClient, atLeast(2)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
                    testContext.completeNow();
                }));
    }

    @Test
    public void testTrafficFilter_denylistedMessagesAreDropped(VertxTestContext testContext) {
        setupDenylistConfig("192.168.1.100");

        // create mixed messages: 2 denylisted (matching ip), 2 normal
        List<Message> denylistedMessages = createOldMessagesWithIp(2, "192.168.1.100");
        List<Message> normalMessages = createOldMessages(2);
        List<Message> allMessages = new ArrayList<>(denylistedMessages);
        allMessages.addAll(normalMessages);
        
        mockSqsToReturnMessages(allMessages);
        mockSqsDeleteSuccess();

        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    JsonObject result = assertJobCompletedWithSuccess(finalStatus);
                    assertEquals(2, result.getInteger("entries_processed"));
                    assertEquals(2, result.getInteger("dropped_requests_processed"));
                    verifyCloudStorageUploaded();
                    verifyDroppedRequestsUploaded();
                    testContext.completeNow();
                }));
    }

    @Test
    public void testTrafficFilter_noDenylistedMessages(VertxTestContext testContext) {
        setupDenylistConfig("192.168.1.100");
        mockSqsToReturnMessages(createOldMessages(2)); // ips don't match denylist
        mockSqsDeleteSuccess();

        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    JsonObject result = assertJobCompletedWithSuccess(finalStatus);
                    assertEquals(2, result.getInteger("entries_processed"));
                    assertEquals(0, result.getInteger("dropped_requests_processed"));
                    verifyDroppedRequestsNotUploaded();
                    testContext.completeNow();
                }));
    }

    @Test
    public void testTrafficFilter_allMessagesDenylisted(VertxTestContext testContext) {
        setupDenylistConfig("192.168.1.100");
        mockSqsToReturnMessages(createOldMessagesWithIp(2, "192.168.1.100")); // all match denylist
        mockSqsDeleteSuccess();

        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    JsonObject result = assertJobCompletedWithSuccess(finalStatus);
                    assertEquals(0, result.getInteger("entries_processed"));
                    assertEquals(2, result.getInteger("dropped_requests_processed"));
                    verifyDroppedRequestsUploaded();
                    verifyCloudStorageNotUploaded();
                    testContext.completeNow();
                }));
    }

    @Test
    public void testTrafficFilter_messagesWithoutClientIp(VertxTestContext testContext) {
        setupDenylistConfig("192.168.1.100");
        
        // create message without client ip (should not be denylisted)
        long oldTime = System.currentTimeMillis() - 400_000;
        List<Message> messages = Arrays.asList(
                createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime, null, null, null, null));
        mockSqsToReturnMessages(messages);
        mockSqsDeleteSuccess();

        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    JsonObject result = assertJobCompletedWithSuccess(finalStatus);
                    assertEquals(1, result.getInteger("entries_processed"));
                    assertEquals(0, result.getInteger("dropped_requests_processed"));
                    testContext.completeNow();
                }));
    }

    @Test
    public void testTrafficFilterConfig_reloadOnEachBatch(VertxTestContext testContext) {
        // initial config with no denylist
        createTrafficConfigFile("""
                {
                    "denylist_requests": []
                }
                """);

        List<Message> messages = createOldMessagesWithIp(1, "192.168.1.100");
        mockSqsToReturnMessages(messages);
        mockSqsDeleteSuccess();

        // first request - should process normally (no denylist)
        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    JsonObject result = assertJobCompletedWithSuccess(finalStatus);
                    assertEquals(1, result.getInteger("entries_processed"));
                    assertEquals(0, result.getInteger("dropped_requests_processed"));

                    // update config to denylist the IP
                    setupDenylistConfig("192.168.1.100");
                    resetMocksForSecondRequest(messages);

                    // second request - should now be denylisted
                    startJobAndPollForCompletion()
                            .onComplete(testContext.succeeding(finalStatus2 -> {
                                JsonObject result2 = assertJobCompletedWithSuccess(finalStatus2);
                                assertEquals(0, result2.getInteger("entries_processed"));
                                assertEquals(1, result2.getInteger("dropped_requests_processed"));
                                testContext.completeNow();
                            }));
                }));
    }

    @Test
    public void testManualOverride_delayedProcessing(VertxTestContext testContext) throws Exception {
        // mock manual override set to DELAYED_PROCESSING
        JsonObject manualOverride = new JsonObject().put("manual_override", "DELAYED_PROCESSING");
        doReturn(new ByteArrayInputStream(manualOverride.encode().getBytes()))
                .when(cloudStorage).download(MANUAL_OVERRIDE_S3_PATH);

        mockSqsToReturnMessages(createOldMessages(2));
        mockSqsDeleteSuccess();

        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    JsonObject result = assertJobCompletedWithHalted(finalStatus, "MANUAL_OVERRIDE_ACTIVE");
                    assertEquals(0, result.getInteger("entries_processed"));
                    assertEquals(0, result.getInteger("deltas_produced"));
                    verifySqsMessagesNotDeleted();
                    testContext.completeNow();
                }));
    }

    @Test
    public void testTrafficCalculator_detectsSpikeInCurrentWindow(VertxTestContext testContext) throws Exception {
        // threshold = baseline * multiplier = 100 * 5 = 500
        // create 600 messages to exceed threshold
        long currentTime = System.currentTimeMillis() / 1000;
        List<Long> timestamps = Arrays.asList(currentTime - 3600, currentTime - 3600 + 1000);
        byte[] deltaFileBytes = createDeltaFileBytes(timestamps);

        reset(cloudStorage);
        setupCloudStorageForSpikeTest(deltaFileBytes);

        // create 600 SQS messages to exceed threshold
        long baseTime = (currentTime - 600) * 1000;
        List<Message> allMessages = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            allMessages.add(createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 
                    baseTime - (i * 100), null, null, "10.0.0." + (i % 256), null));
        }

        mockSqsToReturnMessages(allMessages);
        mockSqsDeleteSuccess();

        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    JsonObject result = assertJobCompletedWithHalted(finalStatus, "CIRCUIT_BREAKER_TRIGGERED");
                    assertEquals(0, result.getInteger("entries_processed"));
                    assertEquals(0, result.getInteger("deltas_produced"));

                    // verify manual override was set to DELAYED_PROCESSING on S3
                    try {
                        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
                        verify(cloudStorage, atLeastOnce()).upload(any(InputStream.class), pathCaptor.capture());
                        boolean overrideSet = pathCaptor.getAllValues().stream()
                                .anyMatch(path -> path.equals("manual-override.json"));
                        assertTrue(overrideSet, "Manual override should be set to DELAYED_PROCESSING after detecting spike");
                    } catch (Exception e) {
                        fail(e);
                    }

                    testContext.completeNow();
                }));
    }

    @Test
    public void testS3UploadFailure_messagesNotDeletedFromSqs(VertxTestContext testContext) throws Exception {
        mockSqsToReturnMessages(createOldMessages(2));
        doThrow(new RuntimeException("S3 upload failed - simulated error"))
                .when(cloudStorage).upload(any(InputStream.class), anyString());

        startJobAndPollForCompletion()
                .onComplete(testContext.succeeding(finalStatus -> {
                    assertJobFailed(finalStatus);
                    assertTrue(finalStatus.getString("error").contains("S3") || 
                                      finalStatus.getString("error").contains("upload"));
                    verifySqsMessagesNotDeleted();
                    testContext.completeNow();
                }));
    }

    @Test
    public void testStatusEndpoint_showsRunningJob(VertxTestContext testContext) throws Exception {
        List<Message> messages = createOldMessages(1);
        CountDownLatch processingLatch = new CountDownLatch(1);

        // mock sqs to wait on latch before returning - keeps job running
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(inv -> {
                    processingLatch.await();
                    return ReceiveMessageResponse.builder().messages(messages).build();
                })
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        mockSqsDeleteSuccess();

        postStartJob()
                .compose(resp -> {
                    assertEquals(202, resp.getInteger("_statusCode"));
                    return getJobStatus();
                })
                .onComplete(testContext.succeeding(status -> {
                    assertEquals("running", status.getString("state"));
                    assertNotNull(status.getString("start_time"));

                    processingLatch.countDown();
                    testContext.completeNow();
                }));
    }

    @Test
    public void testStatusEndpoint_showsFailedJob(VertxTestContext testContext) throws Exception {
        mockSqsToReturnMessages(createOldMessages(1));
        doThrow(new RuntimeException("simulated s3 failure"))
                .when(cloudStorage).upload(any(InputStream.class), anyString());

        startJobAndPollForCompletion()
                .compose(finalStatus -> {
                    assertJobFailed(finalStatus);
                    return getJobStatus();
                })
                .onComplete(testContext.succeeding(status -> {
                    assertJobFailed(status);
                    assertNotNull(status.getString("error"));
                    assertNotNull(status.getString("start_time"));
                    assertNotNull(status.getString("end_time"));
                    assertNotNull(status.getInteger("duration_seconds"));

                    testContext.completeNow();
                }));
    }

    @Test
    public void testDeltaProduceEndpoint_invalidApiKey(VertxTestContext testContext) {
        postWithAuth("wrong-api-key")
                .onComplete(testContext.succeeding(resp -> {
                    assertEquals(401, resp.getInteger("_statusCode"));
                    verifySqsReceiveNotCalled();
                    testContext.completeNow();
                }));
    }
}
