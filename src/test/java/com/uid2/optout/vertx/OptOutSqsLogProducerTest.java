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
    private OptOutCloudSync cloudSync;
    
    private static final String TEST_QUEUE_URL = "https://sqs.test.amazonaws.com/123456789/test";
    private static final String TEST_API_KEY = "test-api-key";
    private static final String VALID_HASH_BASE64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    private static final String VALID_ID_BASE64 = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
    
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
              .put(Const.Config.OptOutInternalApiTokenProp, TEST_API_KEY);
        
        // Mock cloud sync to return proper S3 paths
        when(cloudSync.toCloudPath(anyString()))
                .thenAnswer(inv -> "sqs-delta/delta/" + inv.getArgument(0));
        
        // Mock S3 upload to succeed by default
        doAnswer(inv -> null).when(cloudStorage).upload(any(InputStream.class), anyString());
        
        // Create producer with mock SqsClient
        producer = new OptOutSqsLogProducer(config, cloudStorage, cloudSync, Const.Event.DeltaProduce, sqsClient);
        
        // Deploy verticle
        Async async = context.async();
        vertx.deployVerticle(producer, context.asyncAssertSuccess(v -> async.complete()));
    }
    
    @After
    public void tearDown(TestContext context) {
        if (vertx != null) {
            vertx.close(context.asyncAssertSuccess());
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
                    context.assertEquals("All messages too recent", result.getString("reason"));
                    
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
        
        // Mock SQS to return messages
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(messages).build())
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
                    context.assertTrue(response.getString("message").contains("already running"));
                    
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
}

