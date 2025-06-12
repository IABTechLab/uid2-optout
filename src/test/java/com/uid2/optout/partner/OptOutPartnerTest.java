package com.uid2.optout.partner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.uid2.shared.audit.Audit;
import com.uid2.shared.audit.UidInstanceIdProvider;
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

@RunWith(VertxUnitRunner.class)
public class OptOutPartnerTest {
    private static final String PARAM_UID2 = "uid2";
    private static final String PARAM_TIMESTAMP = "timestamp";
    private final UidInstanceIdProvider uidInstanceIdProvider = new UidInstanceIdProvider("test-instance", "id");

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
        String partnerConfigStr = "{" +
                "      \"name\": \"ttd\",\n" +
                //UID2-3697 intentionally added some upper cases to make sure the url is the same as what's provided by DSP originally
                "      \"url\": \"http://localhost:18083/AdServer/uid2optout\",\n" +
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
        testConfig_expectSuccess(ctx, partnerConfigStr, true);
    }

    @Test
    public void externalHttpSite_expectSuccess(TestContext ctx) throws JsonProcessingException, InvalidPropertiesFormatException {
        testSite_expectSuccess(ctx, "http://httpstat.us/200");
    }

    @Test
    public void externalHttpsSite_expectSuccess(TestContext ctx) throws JsonProcessingException, InvalidPropertiesFormatException {
        testSite_expectSuccess(ctx, "https://httpstat.us/200");
    }

    private void testSite_expectSuccess(TestContext ctx, String site) throws JsonProcessingException, InvalidPropertiesFormatException {
        String partnerConfigStr = "{" +
                "      \"name\": \"ttd\",\n" +
                "      \"url\": \"" + site + "\",\n" +
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
        testConfig_expectSuccess(ctx, partnerConfigStr, false);
    }

    private void testConfig_expectSuccess(TestContext ctx, String partnerConfigStr, boolean createInternalTestServer) throws JsonProcessingException, InvalidPropertiesFormatException {

        EndpointConfig partnerConfig = EndpointConfig.fromJsonString(partnerConfigStr);

        byte[] idHash = OptOutEntry.idHashFromLong(4567);
        byte[] advertisingId = OptOutEntry.idHashFromLong(1234);
        long timestamp = Instant.now().getEpochSecond();
        OptOutEntry entry = new OptOutEntry(idHash, advertisingId, timestamp);

        if (createInternalTestServer) {
            Async async = ctx.async();
            HttpServer server = this.createTestServer(ctx, req -> {
                ctx.assertEquals("/AdServer/uid2optout", req.path());

                String uid2Expected = OptOutEntry.idHashB64FromLong(1234);
                String uid2 = req.getParam(OptOutPartnerTest.PARAM_UID2);
                ctx.assertEquals(uid2Expected, uid2);

                String tsExpected = String.valueOf(timestamp);
                String ts = req.getParam(OptOutPartnerTest.PARAM_TIMESTAMP);
                ctx.assertEquals(tsExpected, ts);

                String hExpected = "Bearer 111-1111111";
                String hServiceIdExpected = "test-instance-id";
                String h = req.getHeader("Authorization");
                String hServiceId = req.getHeader(Audit.UID_INSTANCE_ID_HEADER);
                ctx.assertEquals(hExpected, h);
                ctx.assertEquals(hServiceIdExpected, hServiceId);
                async.complete();
            });
        }

        OptOutPartnerEndpoint remote = new OptOutPartnerEndpoint(vertx, partnerConfig, uidInstanceIdProvider);
        remote.send(entry).onComplete(ctx.asyncAssertSuccess());
    }

    private HttpServer createTestServer(TestContext ctx, Handler<HttpServerRequest> requestValidator) {
        return vertx.createHttpServer()
                .requestHandler(req -> {
                    requestValidator.handle(req);
                    req.response().setStatusCode(200).end();
                })
                .listen(18083, ctx.asyncAssertSuccess());
    }
}
