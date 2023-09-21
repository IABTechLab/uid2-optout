package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.TestUtils;
import com.uid2.optout.web.QuorumWebClient;
import com.uid2.shared.optout.OptOutEntry;
import com.uid2.shared.optout.OptOutUtils;
import com.uid2.shared.vertx.VertxUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

@RunWith(VertxUnitRunner.class)
public class OptOutServiceVerticleTest {
    private static final String INTERNAL_TEST_KEY = "test-operator-key";
    private static final String INTERNAL_OPTOUT_KEY = "test-optout-operator-key";
    private static Vertx vertx;

    @BeforeClass
    public static void suiteSetup(TestContext context) throws Exception {
        vertx = Vertx.vertx();
        JsonObject config = VertxUtils.getJsonConfig(vertx);
        deployLogProducer(context, config)
                .compose(v -> {
                    try {
                        return deployService(context, config);
                    } catch (Exception e) {
                        return Future.failedFuture(e);
                    }
                })
                .onComplete(context.asyncAssertSuccess());
    }

    @AfterClass
    public static void suiteTearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }


    private static Future<String> deployLogProducer(TestContext context, JsonObject config) throws Exception {
        Promise<String> promise = Promise.promise();

        // set data_dir option to use tmpDir during test
        config
                .put(Const.Config.OptOutDataDirProp, OptOutUtils.tmpDir)
                .put(Const.Config.OptOutInternalApiTokenProp, INTERNAL_TEST_KEY)
                .put(Const.Config.OptOutReplicaUris, "http://127.0.0.1:8081/optout/write,http://127.0.0.1:8081/optout/write,http://127.0.0.1:8081/optout/write");

        OptOutLogProducer producer = TestUtils.createOptOutLogProducer(vertx, config);
        vertx.deployVerticle(producer, ar -> promise.handle(ar));
        return promise.future();
    }

    private static Future<String> deployService(TestContext context, JsonObject config) throws Exception {
        Promise<String> promise = Promise.promise();
        OptOutServiceVerticle svc = TestUtils.createOptOutService(vertx, config);
        vertx.deployVerticle(svc, ar -> {
            // set an empty cloud paths
            svc.setCloudPaths(new ArrayList<>());
            promise.handle(ar);
        });
        return promise.future();
    }

    @Test
    public void writeNull_expect400(TestContext context) {
        verifyStatus(context, writeQuery(OptOutUtils.nullHashBytes), 400);
    }

    @Test
    public void writeOnes_expect400(TestContext context) {
        verifyStatus(context, writeQuery(OptOutUtils.onesHashBytes), 400);
    }

    @Test
    public void writeId_expect200(TestContext context) {
        verifyStatus(context, writeQuery(100), 200);
    }

    @Test
    public void getHealthCheck_expectOK(TestContext context) {
        verifyStatus(context, OptOutServiceVerticle.HEALTHCHECK_METHOD, 200);
    }

    @Test
    public void writeMultiple_expect200(TestContext context) {
        Future<Void> f = Future.succeededFuture();
        for (int i = 0; i < 3; ++i) {
            final long id = 1 + i * 100;
            f.compose(v -> verifyStatus(context, writeQuery(id), 200));
        }
        f.onComplete(context.asyncAssertSuccess());
    }

    @Test
    public void writeIdsSerial_expect200(TestContext context) {
        Future<Void> f = Future.succeededFuture();
        for (int i = 0; i < 100; ++i) {
            final long id = 100 + i;
            f.compose(v -> verifyStatus(context, writeQuery(id), 200));
        }
        f.onComplete(context.asyncAssertSuccess());
    }

    @Test
    public void writeIdsParallel_expect200(TestContext context) {
        List<Future> fs = new ArrayList<Future>();
        for (int i = 0; i < 100; ++i) {
            final long id = 100 + i;
            fs.add(verifyStatus(context, writeQuery(id), 200));
        }
        CompositeFuture.all(fs).onComplete(context.asyncAssertSuccess());
    }

    // optout/add forwards request to remote optout/write api endpoints
    @Test
    public void replicateWithoutOptoutRole_expect401(TestContext context) {
        verifyStatus(context, replicateQuery(234), 401);
    }
    @Test
    public void replicate_expect200(TestContext context) {
        verifyStatus(context, replicateQuery(234), 200, INTERNAL_OPTOUT_KEY);
    }

    @Test
    public void testQuorumClient_expectSuccess(TestContext context) {
        String[] uris = new String[3];
        for (int i = 0; i < 3; ++i) {
            uris[i] = String.format("http://127.0.0.1:%d%s", Const.Port.ServicePortForOptOut, OptOutServiceVerticle.WRITE_METHOD);
        }

        QuorumWebClient quorumClient = new QuorumWebClient(vertx, uris);
        quorumClient.get(req -> {
            req.addQueryParam(OptOutServiceVerticle.IDENTITY_HASH, OptOutEntry.idHashB64FromLong(123));
            req.addQueryParam(OptOutServiceVerticle.ADVERTISING_ID, OptOutEntry.idHashB64FromLong(456));
            req.bearerTokenAuthentication(INTERNAL_TEST_KEY);
            return req;
        }).onComplete(context.asyncAssertSuccess());
    }

    @Test
    public void testQuorumClient1Failure_expectSuccess(TestContext context) {
        String[] uris = new String[3];
        for (int i = 0; i < 2; ++i) {
            uris[i] = String.format("http://127.0.0.1:%d%s", Const.Port.ServicePortForOptOut, OptOutServiceVerticle.WRITE_METHOD);
        }
        uris[2] = "http://httpstat.us/404";

        QuorumWebClient quorumClient = new QuorumWebClient(vertx, uris);
        quorumClient.get(req -> {
            req.addQueryParam(OptOutServiceVerticle.IDENTITY_HASH, OptOutEntry.idHashB64FromLong(123));
            req.addQueryParam(OptOutServiceVerticle.ADVERTISING_ID, OptOutEntry.idHashB64FromLong(456));
            req.bearerTokenAuthentication(INTERNAL_TEST_KEY);
            return req;
        }).onComplete(context.asyncAssertSuccess());
    }

    @Test
    public void testQuorumClientAllFailures_expectSuccess(TestContext context) {
        String[] uris = new String[3];
        for (int i = 0; i < 3; ++i) {
            uris[i] = "http://httpstat.us/404";
        }

        QuorumWebClient quorumClient = new QuorumWebClient(vertx, uris);
        quorumClient.get(req -> {
            req.addQueryParam(OptOutServiceVerticle.IDENTITY_HASH, OptOutEntry.idHashB64FromLong(123));
            req.addQueryParam(OptOutServiceVerticle.ADVERTISING_ID, OptOutEntry.idHashB64FromLong(456));
            return req;
        }).onComplete(context.asyncAssertFailure());
    }

    private String writeQuery(long id) {
        return this.writeQuery(OptOutEntry.idHashB64FromLong(id));
    }

    private String writeQuery(String identityHashB64) {
        return this.writeQuery(identityHashB64, identityHashB64);
    }

    private String writeQuery(byte[] identityHash) {
        return this.writeQuery(identityHash, identityHash);
    }

    private String writeQuery(byte[] identityHash, byte[] advertisingId) {
        return this.writeQuery(OptOutUtils.byteArrayToBase64String(identityHash),
                OptOutUtils.byteArrayToBase64String(advertisingId));
    }

    private String writeQuery(String identityHashB64, String advertisingIdB64) {
        return String.format("%s?%s=%s&%s=%s", OptOutServiceVerticle.WRITE_METHOD,
                OptOutServiceVerticle.IDENTITY_HASH,
                identityHashB64,
                OptOutServiceVerticle.ADVERTISING_ID,
                advertisingIdB64);
    }

    private String replicateQuery(long id) {
        return this.replicateQuery(OptOutEntry.idHashB64FromLong(id),
                OptOutEntry.idHashB64FromLong(id));
    }

    private String replicateQuery(String identityHashB64, String advertisingIdB64) {
        return String.format("%s?%s=%s&%s=%s", OptOutServiceVerticle.REPLICATE_METHOD,
                OptOutServiceVerticle.IDENTITY_HASH,
                identityHashB64,
                OptOutServiceVerticle.ADVERTISING_ID,
                advertisingIdB64);
    }
    private Future<Void> verifyStatus(TestContext context, String pq, int status) {
        return verifyStatus(context, pq, status, INTERNAL_TEST_KEY);
    }

    private Future<Void> verifyStatus(TestContext context, String pq, int status, String token) {
        Promise<Void> promise = Promise.promise();
        Async async = context.async();
        int port = Const.Port.ServicePortForOptOut;
        vertx.createHttpClient()
                .request(HttpMethod.GET, port, "127.0.0.1", pq)
                .compose(req -> req.putHeader("Authorization", "Bearer " + token)
                        .send()
                        .compose(resp -> {
                            context.assertEquals(status, resp.statusCode());
                            async.complete();
                            promise.complete();
                            return resp.body();
                        }));
        return promise.future();
    }

    private Future<Void> verifyStatusAndBody(TestContext context, String pq, int status, String body) {
        Promise<Void> promise = Promise.promise();
        Async async = context.async();
        int port = Const.Port.ServicePortForOptOut;
        vertx.createHttpClient()
                .request(HttpMethod.GET, port, "127.0.0.1", pq)
                .compose(req -> req.putHeader("Authorization", "Bearer " + INTERNAL_TEST_KEY)
                        .send()
                        .compose(resp -> {
                            context.assertEquals(status, resp.statusCode());
                            resp.handler(respBody -> {
                                context.assertEquals(body, respBody.toString());
                                async.complete();
                                promise.complete();
                            });
                            return resp.body();
                        }));
        return promise.future();
    }
}
