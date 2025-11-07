package com.uid2.optout.partner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.uid2.shared.optout.OptOutEntry;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Instant;
import java.util.InvalidPropertiesFormatException;
import java.util.Random;

@RunWith(VertxUnitRunner.class)
public class OptOutPartnerTest {
    private static final String PARAM_UID2 = "uid2";
    private static final String PARAM_TIMESTAMP = "timestamp";

    private Vertx vertx;

    @Before
    public void setup(TestContext ctx) {
        vertx = Vertx.vertx();
    }

    @After
    public void tearDown(TestContext ctx) {
        vertx.close(ctx.asyncAssertSuccess());
    }

    @Test
    public void internalSite_expectSuccess(TestContext ctx) throws JsonProcessingException, InvalidPropertiesFormatException {
        int port = 10000 + new Random().nextInt(50000);
        String partnerConfigStr = "{" +
                "      \"name\": \"ttd\",\n" +
                //UID2-3697 intentionally added some upper cases to make sure the url is the same as what's provided by DSP originally
                "      \"url\": \"http://localhost:" + port + "/AdServer/uid2optout\",\n" +
                "      \"method\": \"GET\",\n" +
                "      \"query_params\": [\n" +
                "        \"uid2=${ADVERTISING_ID}\",\n" +
                "        \"timestamp=${OPTOUT_EPOCH}\"\n" +
                "      ],\n" +
                "      \"additional_headers\": [\n" +
                "        \"Authorization: Bearer 111-1111111\"\n" +
                "      ],\n" +
                "      \"retry_count\": 600,\n" +
                "      \"retry_backoff_ms\": 6000" +
                "}";
        testConfig_expectSuccess(ctx, partnerConfigStr, true, port);
    }

    @Test
    public void simpleHttpEndpoint_expectSuccess(TestContext ctx) throws JsonProcessingException, InvalidPropertiesFormatException {
        // Test a simple HTTP endpoint with minimal configuration
        int port = 10000 + new Random().nextInt(50000);
        String partnerConfigStr = "{" +
                "      \"name\": \"simple-partner\",\n" +
                "      \"url\": \"http://localhost:" + port + "/optout\",\n" +
                "      \"method\": \"GET\",\n" +
                "      \"query_params\": [\n" +
                "        \"id=${ADVERTISING_ID}\"\n" +
                "      ],\n" +
                "      \"retry_count\": 3,\n" +
                "      \"retry_backoff_ms\": 100" +
                "}";

        byte[] idHash = OptOutEntry.idHashFromLong(4567);
        byte[] advertisingId = OptOutEntry.idHashFromLong(1234);
        long timestamp = Instant.now().getEpochSecond();
        OptOutEntry entry = new OptOutEntry(idHash, advertisingId, timestamp);

        Async async = ctx.async();
        vertx.createHttpServer()
                .requestHandler(req -> {
                    ctx.assertEquals("/optout", req.path());
                    
                    String idExpected = OptOutEntry.idHashB64FromLong(1234);
                    String id = req.getParam("id");
                    ctx.assertEquals(idExpected, id);
                    
                    req.response().setStatusCode(200).end();
                    async.complete();
                })
                .listen(port, ctx.asyncAssertSuccess());

        EndpointConfig partnerConfig = EndpointConfig.fromJsonString(partnerConfigStr);
        OptOutPartnerEndpoint remote = new OptOutPartnerEndpoint(vertx, partnerConfig);
        remote.send(entry).onComplete(ctx.asyncAssertSuccess());
    }

    @Test
    public void customPathAndHeaders_expectSuccess(TestContext ctx) throws JsonProcessingException, InvalidPropertiesFormatException {
        // Test an endpoint with custom path and headers
        int port = 10000 + new Random().nextInt(50000);
        String partnerConfigStr = "{" +
                "      \"name\": \"custom-partner\",\n" +
                "      \"url\": \"http://localhost:" + port + "/api/v1/optout\",\n" +
                "      \"method\": \"GET\",\n" +
                "      \"query_params\": [\n" +
                "        \"user_id=${ADVERTISING_ID}\",\n" +
                "        \"ts=${OPTOUT_EPOCH}\"\n" +
                "      ],\n" +
                "      \"additional_headers\": [\n" +
                "        \"X-Custom-Header: test-value\"\n" +
                "      ],\n" +
                "      \"retry_count\": 5,\n" +
                "      \"retry_backoff_ms\": 200" +
                "}";

        byte[] idHash = OptOutEntry.idHashFromLong(9999);
        byte[] advertisingId = OptOutEntry.idHashFromLong(5555);
        long timestamp = Instant.now().getEpochSecond();
        OptOutEntry entry = new OptOutEntry(idHash, advertisingId, timestamp);

        Async async = ctx.async();
        vertx.createHttpServer()
                .requestHandler(req -> {
                    ctx.assertEquals("/api/v1/optout", req.path());
                    
                    String userIdExpected = OptOutEntry.idHashB64FromLong(5555);
                    String userId = req.getParam("user_id");
                    ctx.assertEquals(userIdExpected, userId);
                    
                    String tsExpected = String.valueOf(timestamp);
                    String ts = req.getParam("ts");
                    ctx.assertEquals(tsExpected, ts);
                    
                    String customHeader = req.getHeader("X-Custom-Header");
                    ctx.assertEquals("test-value", customHeader);
                    
                    req.response().setStatusCode(200).end();
                    async.complete();
                })
                .listen(port, ctx.asyncAssertSuccess());

        EndpointConfig partnerConfig = EndpointConfig.fromJsonString(partnerConfigStr);
        OptOutPartnerEndpoint remote = new OptOutPartnerEndpoint(vertx, partnerConfig);
        remote.send(entry).onComplete(ctx.asyncAssertSuccess());
    }

    private void testConfig_expectSuccess(TestContext ctx, String partnerConfigStr, boolean createInternalTestServer, int port) throws JsonProcessingException, InvalidPropertiesFormatException {

        EndpointConfig partnerConfig = EndpointConfig.fromJsonString(partnerConfigStr);

        byte[] idHash = OptOutEntry.idHashFromLong(4567);
        byte[] advertisingId = OptOutEntry.idHashFromLong(1234);
        long timestamp = Instant.now().getEpochSecond();
        OptOutEntry entry = new OptOutEntry(idHash, advertisingId, timestamp);

        if (createInternalTestServer) {
            Async async = ctx.async();
            this.createTestServer(ctx, port, req -> {
                ctx.assertEquals("/AdServer/uid2optout", req.path());

                String uid2Expected = OptOutEntry.idHashB64FromLong(1234);
                String uid2 = req.getParam(OptOutPartnerTest.PARAM_UID2);
                ctx.assertEquals(uid2Expected, uid2);

                String tsExpected = String.valueOf(timestamp);
                String ts = req.getParam(OptOutPartnerTest.PARAM_TIMESTAMP);
                ctx.assertEquals(tsExpected, ts);

                String hExpected = "Bearer 111-1111111";
                String h = req.getHeader("Authorization");
                ctx.assertEquals(hExpected, h);
                async.complete();
            });
        }

        OptOutPartnerEndpoint remote = new OptOutPartnerEndpoint(vertx, partnerConfig);
        remote.send(entry).onComplete(ctx.asyncAssertSuccess());
    }

    private HttpServer createTestServer(TestContext ctx, int port, Handler<HttpServerRequest> requestValidator) {
        return vertx.createHttpServer()
                .requestHandler(req -> {
                    requestValidator.handle(req);
                    req.response().setStatusCode(200).end();
                })
                .listen(port, ctx.asyncAssertSuccess());
    }
}
