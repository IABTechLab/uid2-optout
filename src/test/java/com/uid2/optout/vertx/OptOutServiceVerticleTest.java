package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.TestUtils;
import com.uid2.shared.optout.OptOutUtils;
import com.uid2.shared.vertx.VertxUtils;
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

@RunWith(VertxUnitRunner.class)
public class OptOutServiceVerticleTest {
    private static final String INTERNAL_TEST_KEY = "test-operator-key";
    private static Vertx vertx;

    @BeforeClass
    public static void suiteSetup(TestContext context) throws Exception {
        vertx = Vertx.vertx();
        JsonObject config = VertxUtils.getJsonConfig(vertx);
        deployService(context, config)
                .onComplete(context.asyncAssertSuccess());
    }

    @AfterClass
    public static void suiteTearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    private static Future<String> deployService(TestContext context, JsonObject config) throws Exception {
        Promise<String> promise = Promise.promise();
        
        config
                .put(Const.Config.OptOutDataDirProp, OptOutUtils.tmpDir)
                .put(Const.Config.OptOutInternalApiTokenProp, INTERNAL_TEST_KEY);

        OptOutServiceVerticle svc = TestUtils.createOptOutService(vertx, config);
        vertx.deployVerticle(svc, ar -> {
            // set an empty cloud paths
            svc.setCloudPaths(new ArrayList<>());
            promise.handle(ar);
        });
        return promise.future();
    }

    @Test
    public void getHealthCheck_expectOK(TestContext context) {
        verifyStatus(context, Endpoints.OPS_HEALTHCHECK.toString(), 200);
    }

    @Test
    public void replicateWithoutAuth_expect401(TestContext context) {
        verifyStatus(context, replicateQuery(234), 401, null);
    }

    private String replicateQuery(long id) {
        return this.replicateQuery(
                com.uid2.shared.optout.OptOutEntry.idHashB64FromLong(id),
                com.uid2.shared.optout.OptOutEntry.idHashB64FromLong(id));
    }

    private String replicateQuery(String identityHashB64, String advertisingIdB64) {
        return String.format("%s?%s=%s&%s=%s", Endpoints.OPTOUT_REPLICATE,
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
                .compose(req -> {
                    if (token != null) {
                        req.putHeader("Authorization", "Bearer " + token);
                    }
                    return req.send()
                            .compose(resp -> {
                                context.assertEquals(status, resp.statusCode());
                                async.complete();
                                promise.complete();
                                return resp.body();
                            });
                });
        return promise.future();
    }
}
