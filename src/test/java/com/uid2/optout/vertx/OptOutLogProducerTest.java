package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.TestUtils;
import com.uid2.shared.optout.*;
import com.uid2.shared.vertx.VertxUtils;
import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@RunWith(VertxUnitRunner.class)
public class OptOutLogProducerTest {
    // set data_dir option to use tmpDir during test
    private Vertx vertx;

    public OptOutLogProducerTest() throws IOException {
    }

    @Before
    public void setup(TestContext context) throws Exception {
        vertx = Vertx.vertx();
        JsonObject config = VertxUtils.getJsonConfig(vertx);
        // set data_dir option to use tmpDir during test
        config.put(Const.Config.OptOutDataDirProp, OptOutUtils.tmpDir);
        OptOutLogProducer producer = TestUtils.createOptOutLogProducer(vertx, config);
        vertx.deployVerticle(producer, context.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext context) {
        // context.assertTrue(OptOutLogProducer.instance.isHealthy());
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void callLogProduce_expectLogProduced(TestContext ctx) {
        long[] p = OptOutUtils.toLongArray(0);
        long[] n = OptOutUtils.toLongArray(1, 2, 3);
        verifySuccessLogProduce(ctx).onFailure(ex -> ctx.fail(ex))
            .onSuccess(logFile -> verifyLog(ctx, logFile, p, n));
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
    public void addEntry_verifyResults(TestContext ctx) {
        verifySuccessEntryAdd(ctx, OptOutEntry.idHashB64FromLong(1));
    }

    @Test
    public void addEntriesParallel_verifyResults(TestContext ctx) {
        for (int i = 0; i < 100; ++i) {
            verifySuccessEntryAdd(ctx, OptOutEntry.idHashB64FromLong(i));
        }
    }

    @Test
    public void addEntriesSerial_verifyResults(TestContext ctx) {
        Future<Void> f = Future.succeededFuture();
        for (int i = 0; i < 100; ++i) {
            final long id = i;
            f = f.compose(v -> verifySuccessEntryAdd(ctx, OptOutEntry.idHashB64FromLong(id)));
        }
        f.onComplete(ctx.asyncAssertSuccess());
    }

    @Test
    public void addEntriesParallelAndProduce_verifyResults(TestContext ctx) {
        List<Future> fs = new ArrayList<>();
        List<Long> p = new ArrayList<>();
        List<Long> n = new ArrayList<>();
        for (long i = 0; i < 100; ++i) {
            p.add(i);
            n.add(1000 + i);
            fs.add(verifySuccessEntryAdd(ctx, OptOutEntry.idHashB64FromLong(i)));
        }

        CompositeFuture.all(fs)
            .compose(v -> verifySuccessLogProduce(ctx))
            .onFailure(ex -> ctx.fail(ex))
            .onSuccess(logFile -> verifyLog(ctx, logFile, OptOutUtils.toArray(p), OptOutUtils.toArray(n)));
    }

    @Test
    public void addEntriesSerialAndProduce_verifyResults(TestContext ctx) {
        Future<Void> f = Future.succeededFuture();
        List<Long> p = new ArrayList<>();
        List<Long> n = new ArrayList<>();
        for (long i = 0; i < 100; ++i) {
            p.add(i);
            n.add(1000 + i);
            final long id = i;
            f = f.compose(v -> verifySuccessEntryAdd(ctx, OptOutEntry.idHashB64FromLong(id)));
        }

        f.compose(v -> verifySuccessLogProduce(ctx))
            .onFailure(ex -> ctx.fail(ex))
            .onSuccess(logFile -> verifyLog(ctx, logFile, OptOutUtils.toArray(p), OptOutUtils.toArray(n)));
    }

    @Test
    public void shutdown_verifyLogProduced(TestContext ctx) {
        verifySuccessLogProduceOnShutdown(ctx).onComplete(ctx.asyncAssertSuccess());
    }

    @Test
    public void shutdownAfterEntriesAdded_verifyLogProduced(TestContext ctx) {
        List<Future> fs = new ArrayList<>();
        for (long i = 0; i < 100; ++i) {
            fs.add(verifyReceivedEntryAdd(ctx, OptOutEntry.idHashB64FromLong(i)));
        }

        CompositeFuture.all(fs)
            .compose(v -> verifySuccessLogProduceOnShutdown(ctx))
            .onComplete(ctx.asyncAssertSuccess());
    }

    @Test
    public void shutdownBeforeEntriesAdded_verifyLogProduced(TestContext ctx) {
        List<Future> fs = new ArrayList<>();
        fs.add(verifySuccessLogProduceOnShutdown(ctx));
        for (long i = 0; i < 100; ++i) {
            fs.add(verifyReceivedEntryAdd(ctx, OptOutEntry.idHashB64FromLong(i)));
        }

        CompositeFuture.all(fs)
            .onComplete(ctx.asyncAssertSuccess());
    }

    @Test
    public void shutdownWhileEntriesAdded_verifyLogProduced(TestContext ctx) {
        List<Future> fs = new ArrayList<>();
        for (long i = 0; i < 100; ++i) {
            fs.add(verifyReceivedEntryAdd(ctx, OptOutEntry.idHashB64FromLong(i)));
        }
        fs.add(verifySuccessLogProduceOnShutdown(ctx));
        for (long i = 0; i < 100; ++i) {
            fs.add(verifyReceivedEntryAdd(ctx, OptOutEntry.idHashB64FromLong(100 + i)));
        }

        CompositeFuture.all(fs)
            .onComplete(ctx.asyncAssertSuccess());
    }

    @Test
    public void internal_testLog(TestContext ctx) throws IOException {
        long[] p = OptOutUtils.toLongArray(1, 2, 3);
        long[] n = OptOutUtils.toLongArray(4, 5, 6);
        String log = TestUtils.newDeltaFile(p);
        verifyLog(ctx, log, p, n);
    }

    @Test
    public void internal_testSnapshot(TestContext ctx) throws IOException {
        long[] p = OptOutUtils.toLongArray(1, 2, 3);
        long[] n = OptOutUtils.toLongArray(4, 5, 6);
        String snapshot = TestUtils.newPartitionFile(p);
        verifyLog(ctx, snapshot, p, n);
    }

    private Future<Void> verifyReceivedEntryAdd(TestContext ctx, String identityHash) {
        return this.verifyReceivedEntryAdd(ctx, identityHash, identityHash);
    }

    private Future<Void> verifyReceivedEntryAdd(TestContext ctx, String identityHash, String advertisingId) {
        Promise<Void> promise = Promise.promise();
        long ts = Instant.now().getEpochSecond();
        String msg = identityHash + "," + advertisingId + "," + String.valueOf(ts);
        Async async = ctx.async();
        DeliveryOptions opts = new DeliveryOptions();
        opts.setSendTimeout(500);
        vertx.eventBus().request(Const.Event.EntryAdd, msg, opts, ar -> {
            // System.out.format("msg: %s, isFailed: %b\n", msg, ar.failed());
            if (ar.failed()) {
                promise.complete();
            } else if (!ar.result().body().equals(true) && !ar.result().body().equals(false)) {
                promise.fail("Unknonwn msg resp: " + ar.result().body());
                ctx.fail();
            } else {
                promise.complete();
            }
            async.complete();
        });
        return promise.future();
    }

    private Future<Void> verifySuccessEntryAdd(TestContext ctx, String identityHash) {
        // using the same value for identity_hash and advertising_id
        return this.verifySuccessEntryAdd(ctx, identityHash, identityHash);
    }

    private Future<Void> verifySuccessEntryAdd(TestContext ctx, String identityHash, String advertisingId) {
        Promise<Void> promise = Promise.promise();
        String msg = identityHash + "," + advertisingId + "," + Instant.now().getEpochSecond();
        Async async = ctx.async();
        vertx.eventBus().request(Const.Event.EntryAdd, msg, ar -> {
            if (ar.failed()) {
                promise.fail(ar.cause());
                ctx.fail();
            } else if (!ar.result().body().equals(true)) {
                promise.fail("Unknonwn msg resp: " + ar.result());
                ctx.fail();
            } else {
                promise.complete();
            }
            async.complete();
        });
        return promise.future();
    }

    private Future<String> verifySuccessLogProduce(TestContext ctx) {
        Promise<String> promise = Promise.promise();
        Async async = ctx.async();

        MessageConsumer<String> c = vertx.eventBus().consumer(Const.Event.DeltaProduced);
        c.handler(m -> {
            String newLog = m.body();
            ctx.assertTrue(newLog != null);
            ctx.assertTrue(OptOutUtils.isDeltaFile(newLog));

            Path newLogPath = Paths.get(newLog);
            ctx.assertTrue(Files.exists(newLogPath));
            File newLogFile = new File(newLog);
            long fileSize = newLogFile.length();

            // file size must be multiples of optout entry size
            ctx.assertTrue(fileSize > 0);
            ctx.assertTrue(0 == (fileSize % OptOutConst.EntrySize));

            c.unregister(); // unregister this consumer once validation is done
            async.complete();
            promise.complete(newLog);
        });

        vertx.eventBus().send(Const.Event.DeltaProduce, null);
        return promise.future();
    }

    private Future<String> verifySuccessLogProduceOnShutdown(TestContext ctx) {
        Promise<String> promise = Promise.promise();
        Async async = ctx.async();

        MessageConsumer<String> c = vertx.eventBus().consumer(Const.Event.DeltaProduced);
        c.handler(m -> {
            String newLog = m.body();
            ctx.assertTrue(newLog != null);
            ctx.assertTrue(OptOutUtils.isDeltaFile(newLog));

            Path newLogPath = Paths.get(newLog);
            ctx.assertTrue(Files.exists(newLogPath));
            File newLogFile = new File(newLog);
            long fileSize = newLogFile.length();

            // file size must be multiples of optout entry size
            ctx.assertTrue(fileSize > 0);
            ctx.assertTrue(0 == (fileSize % OptOutConst.EntrySize));

            c.unregister(); // unregister this consumer once validation is done
            async.complete();
            promise.complete(newLog);
        });

        vertx.deploymentIDs().forEach(vertx::undeploy);
        return promise.future();
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

    private void verifyLog(TestContext ctx, String logFile, long[] positiveIds, long[] negativeIds) {
        try {
            Set<Long> pSet = OptOutUtils.toSet(positiveIds);
            Set<Long> nSet = OptOutUtils.toSet(negativeIds);
            byte[] data = Files.readAllBytes(Paths.get(logFile));
            OptOutCollection s = new OptOutCollection(data);

            s.forEach(e -> {
                if (e.isSpecialHash()) return;
                long idHash = e.idHashAsLong();
                long adsId = e.advertisingIdAsLong();
                System.out.format("check idHash %d, adsId %d\n", idHash, adsId);
                ctx.assertTrue(pSet.contains(idHash));
                ctx.assertFalse(nSet.contains(idHash));
                ctx.assertTrue(pSet.contains(adsId));
                ctx.assertFalse(nSet.contains(adsId));
            });
        } catch (IOException ex) {
            ctx.fail(ex);
        }
    }

    private void verifySnapshot(TestContext ctx, String snapshotFile, long[] positiveIds, long[] negativeIds) {
        try {
            Set<Long> pSet = OptOutUtils.toSet(positiveIds);
            Set<Long> nSet = OptOutUtils.toSet(negativeIds);
            byte[] data = Files.readAllBytes(Paths.get(snapshotFile));
            OptOutPartition s = new OptOutPartition(data);

            // verify all positive ids are contained in snapshot
            for (long id : positiveIds) {
                // System.out.format("pos: %d\n", id);
                ctx.assertTrue(s.contains(OptOutEntry.idHashFromLong(id)));
            }

            // verify all negative ids are not contained in snapshot
            for (long id : negativeIds) {
                // System.out.format("neg: %d\n", id);
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
