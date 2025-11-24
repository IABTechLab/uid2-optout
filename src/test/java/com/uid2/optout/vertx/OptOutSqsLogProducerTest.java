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
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for OptOutSqsLogProducer deltaproduce endpoint.
 * Requires Java 21 for Mockito compatibility with AWS SDK.
 */
@RunWith(VertxUnitRunner.class)
public class OptOutSqsLogProducerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutSqsLogProducerTest.class);
    
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
    private static final String MANUAL_OVERRIDE_S3_PATH = "manual-override.json";
    private static final String S3_DELTA_PREFIX = "sqs-delta";
    
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
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("success", response.getString("status"));
                    
                    // Should process 2 normal entries
                    context.assertEquals(2, response.getInteger("entries_processed"));
                    
                    // Should have 2 dropped requests
                    context.assertEquals(2, response.getInteger("dropped_requests_processed"));
                    
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
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("success", response.getString("status"));
                    context.assertEquals(2, response.getInteger("entries_processed"));
                    context.assertEquals(0, response.getInteger("dropped_requests_processed"));
                    
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
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("success", response.getString("status"));
                    
                    // No entries processed (all blacklisted)
                    context.assertEquals(0, response.getInteger("entries_processed"));
                    
                    // All messages dropped
                    context.assertEquals(2, response.getInteger("dropped_requests_processed"));
                    
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
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("success", response.getString("status"));
                    
                    // Message should be processed (not blacklisted due to missing IP)
                    context.assertEquals(1, response.getInteger("entries_processed"));
                    context.assertEquals(0, response.getInteger("dropped_requests_processed"));
                    
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
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals(1, response.getInteger("entries_processed"));
                    context.assertEquals(0, response.getInteger("dropped_requests_processed"));
                    
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
                                    context.assertEquals(200, resp.statusCode());
                                    return resp.body();
                                })
                                .onComplete(context.asyncAssertSuccess(body2 -> {
                                    JsonObject response2 = new JsonObject(body2.toString());
                                    // Now should be blacklisted
                                    context.assertEquals(0, response2.getInteger("entries_processed"));
                                    context.assertEquals(1, response2.getInteger("dropped_requests_processed"));
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
        
        // Setup - traffic calc config with no whitelist
        String trafficCalcConfig = """
                {
                    "traffic_calc_whitelist_ranges": []
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
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("success", response.getString("status"));
                    
                    // Should process messages normally (DEFAULT status)
                    context.assertEquals(2, response.getInteger("entries_processed"));
                    context.assertTrue(response.getInteger("deltas_produced") >= 1);
                    
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
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("success", response.getString("status"));
                    
                    // Should not process anything - manual override checked at start
                    context.assertEquals(0, response.getInteger("entries_processed"));
                    context.assertEquals(0, response.getInteger("deltas_produced"));
                    
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
        
        // Setup - SQS messages, 4 messages in 4 different windows
        long oldTime = (t - 600) * 1000;
        List<Message> messages = Arrays.asList(
            createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime, null, null, "10.0.0.1", null), 
            createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime - 300*1000, null, null, "10.0.0.2", null), 
            createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime - 600*1000, null, null, "10.0.0.3", null), 
            createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, oldTime - 900*1000, null, null, "10.0.0.4", null)
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
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("success", response.getString("status"));
                    
                    // Should produce 4 deltas from 4 messages
                    context.assertEquals(4, response.getInteger("entries_processed"));
                    context.assertEquals(4, response.getInteger("deltas_produced"));
                    
                    verify(sqsClient, atLeast(4)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
                    
                    async.complete();
                }));
    }
    
    @Test
    public void testTrafficCalculator_detectsSpikeInCurrentWindow(TestContext context) throws Exception {
        Async async = context.async();
        
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
        
        // Setup SQS messages in 3 different 5-minute windows
        long oldTime = (t - 600) * 1000; // 10 minutes ago
        
        // Window 1 (oldest): Low traffic (2 messages) - matches baseline, should process
        long window1Time = oldTime - 600*1000; // 20 minutes ago
        Message msg1a = createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, window1Time, null, null, "10.0.0.1", null);
        Message msg1b = createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, window1Time - 1000, null, null, "10.0.0.2", null);
        
        // Window 2: High traffic spike (50 messages) - causes 50/2 = 25x baseline!
        // NOTE: The traffic calculator is called with currentDeltaMessages (window 2's messages),
        // so it calculates the spike AFTER processing all of window 2's messages (but the delta is not uploaded).
        List<Message> window2Messages = new ArrayList<>();
        long window2Time = oldTime - 300*1000; // 15 minutes ago (5 min after window1)
        for (int i = 0; i < 50; i++) {
            window2Messages.add(createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, 
                    window2Time - (i * 1000), null, null, "10.0.1." + (i % 256), null));
        }
        
        // Window 3: Messages that should NOT be processed (processing stopped after window 2)
        long window3Time = oldTime; // 10 minutes ago (5 min after window2)
        Message msg3a = createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, window3Time, null, null, "10.0.2.1", null);
        Message msg3b = createMessage(VALID_HASH_BASE64, VALID_ID_BASE64, window3Time - 1000, null, null, "10.0.2.2", null);
        
        // Combine all messages (oldest first for SQS ordering)
        List<Message> allMessages = new ArrayList<>();
        allMessages.add(msg1a);
        allMessages.add(msg1b);
        allMessages.addAll(window2Messages);
        allMessages.add(msg3a);
        allMessages.add(msg3b);
        
        // Mock SQS operations
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(allMessages).build())
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
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("success", response.getString("status"));
                    
                    // Expected behavior:
                    // Window 1: Processes 2 msgs, uploads delta, calculates DEFAULT (2 vs baseline 2), continues
                    // Window 2: Reads all 50 msgs, calculates DELAYED_PROCESSING, STOPS before upload
                    // Window 3: Never processed
                    //
                    // NOTE: Traffic calculator is called with messages from the window JUST processed,
                    // so it detects the spike AFTER processing window 2's messages but BEFORE uploading them.
                    // Result: 52 entries processed (2 + 50), but only 1 delta uploaded (window 1 only), 52 messages remain in queue.
                    context.assertEquals(52, response.getInteger("entries_processed"));
                    context.assertEquals(1, response.getInteger("deltas_produced"));
                    
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
                    context.assertEquals(200, resp.statusCode());
                    return resp.body();
                })
                .onComplete(context.asyncAssertSuccess(body -> {
                    JsonObject response = new JsonObject(body.toString());
                    context.assertEquals("success", response.getString("status"));
                    
                    // Should process normally with traffic calc (no override)
                    context.assertEquals(1, response.getInteger("entries_processed"));
                    
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

