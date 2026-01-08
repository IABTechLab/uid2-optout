package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.TestUtils;
import com.uid2.shared.optout.*;
import com.uid2.shared.vertx.VertxUtils;
import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

@RunWith(VertxUnitRunner.class)
public class OptOutLogProducerTest {
    private Vertx vertx;

    public OptOutLogProducerTest() throws IOException {
    }

    @Before
    public void setup(TestContext context) throws Exception {
        vertx = Vertx.vertx();
        JsonObject config = VertxUtils.getJsonConfig(vertx);
        // set data_dir option to use tmpDir during test
        config.put(Const.Config.OptOutDataDirProp, OptOutUtils.tmpDir);
        OptOutLogProducer producer = new OptOutLogProducer(config);
        vertx.deployVerticle(producer, context.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void callSnapshotProduce_expectSnapshotProduced(TestContext ctx) {
        String log1 = TestUtils.newDeltaFile(1, 2, 3);
        String log2 = TestUtils.newDeltaFile(3, 4, 5);
        String log3 = TestUtils.newDeltaFile(5, 6, 7);
        String log4 = TestUtils.newDeltaFile(7, 8, 9);
        String msg = OptOutUtils.toJson(log1, log2, log3, log4);

        long[] p = OptOutUtils.toLongArray(1, 2, 3, 4, 5, 6, 7, 8, 9);
        long[] n = OptOutUtils.toLongArray(10, 11, 12, 13);
        verifySuccessSnapshotProduce(ctx, msg).onFailure(ex -> ctx.fail(ex))
            .onSuccess(logFile -> verifySnapshot(ctx, logFile, p, n));
    }

    @Test
    public void internal_testSnapshot(TestContext ctx) throws IOException {
        long[] p = OptOutUtils.toLongArray(1, 2, 3);
        long[] n = OptOutUtils.toLongArray(4, 5, 6);
        String snapshot = TestUtils.newPartitionFile(p);
        verifySnapshot(ctx, snapshot, p, n);
    }

    private Future<String> verifySuccessSnapshotProduce(TestContext ctx, String msg) {
        Promise<String> promise = Promise.promise();
        Async async = ctx.async();

        MessageConsumer<String> c = vertx.eventBus().consumer(Const.Event.PartitionProduced);
        c.handler(m -> {
            String newSnap = m.body();
            ctx.assertTrue(newSnap != null);
            ctx.assertTrue(OptOutUtils.isPartitionFile(newSnap));
            c.unregister(); // unregister this consumer once validation is done
            async.complete();
            promise.complete(newSnap);
        });

        vertx.eventBus().send(Const.Event.PartitionProduce, msg);
        return promise.future();
    }

    private void verifySnapshot(TestContext ctx, String snapshotFile, long[] positiveIds, long[] negativeIds) {
        try {
            Set<Long> pSet = OptOutUtils.toSet(positiveIds);
            Set<Long> nSet = OptOutUtils.toSet(negativeIds);
            byte[] data = Files.readAllBytes(Paths.get(snapshotFile));
            OptOutPartition s = new OptOutPartition(data);

            // verify all positive ids are contained in snapshot
            for (long id : positiveIds) {
                ctx.assertTrue(s.contains(OptOutEntry.idHashFromLong(id)));
            }

            // verify all negative ids are not contained in snapshot
            for (long id : negativeIds) {
                ctx.assertFalse(s.contains(OptOutEntry.idHashFromLong(id)));
            }

            s.forEach(e -> {
                long id = e.idHashAsLong();
                // verify each id in the snapshot, is in positive set, not negative set
                ctx.assertTrue(pSet.contains(id));
                ctx.assertFalse(nSet.contains(id));
            });
        } catch (IOException ex) {
            ctx.fail(ex);
        }
    }
}
