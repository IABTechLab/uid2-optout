package com.uid2.optout;

import com.uid2.optout.vertx.OptOutLogProducer;
import com.uid2.optout.vertx.OptOutServiceVerticle;
import com.uid2.optout.vertx.TestOperatorKeyProvider;
import com.uid2.shared.optout.*;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TestUtils {
    public static OptOutLogProducer createOptOutLogProducer(Vertx vertx, JsonObject config) throws Exception {
        return new OptOutLogProducer(config);
    }

    public static OptOutServiceVerticle createOptOutService(Vertx vertx, JsonObject config) throws Exception {
        return new OptOutServiceVerticle(vertx, new TestOperatorKeyProvider(), null, config);
    }

    public static OptOutEntry[] toEntries(long... ids) {
        long now = OptOutUtils.nowEpochSeconds();
        return Arrays.stream(ids).boxed().map(id -> OptOutEntry.newTestEntry(id, now))
            .toArray(OptOutEntry[]::new);
    }

    public static HashSet<OptOutEntry> toEntrySet(long... ids) {
        return OptOutUtils.toSet(toEntries(ids));
    }

    public static Set<byte[]> toIdentityHashSet(long... ids) {
        return Arrays.stream(ids).boxed().map(id -> OptOutEntry.idHashFromLong(id))
            .collect(Collectors.toSet());
    }

    public static String newSuffix() {
        return String.format("_%s_%08x.dat", OptOutUtils.timestampNowEscaped(), OptOutUtils.rand.nextInt());
    }

    public static String newDeltaFileName() {
        try {
            Path tmpFile = Files.createTempFile(OptOutUtils.prefixDeltaFile, newSuffix());
            return tmpFile.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String newDeltaFile(List<Long> list) {
        return TestUtils.newDeltaFile(OptOutUtils.toArray(list));
    }

    public static String newDeltaFile(long... ids) {
        return newDeltaFile(TestUtils.toEntries(ids));
    }

    public static String newDeltaFile(Path path, long... ids) {
        return newDeltaFile(TestUtils.toEntries(ids), path);
    }

    public static String newDeltaFile(OptOutEntry[] entries) {
        Path tmpFile;
        try {
            tmpFile = Files.createTempFile(OptOutUtils.prefixDeltaFile, newSuffix());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return newDeltaFile(entries, tmpFile);
    }

    public static String newDeltaFile(OptOutEntry[] entries, Path path) {
        try {
            OptOutCollection store = new OptOutCollection(entries);
            Files.write(path, store.getStore(), StandardOpenOption.CREATE);
            return path.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String newPartitionFileName() {
        try {
            Path tmpFile = Files.createTempFile(OptOutUtils.prefixPartitionFile, newSuffix());
            return tmpFile.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String newPartitionFile(List<Long> list) {
        return TestUtils.newPartitionFile(OptOutUtils.toArray(list));
    }

    public static String newPartitionFile(long... ids) {
        return newPartitionFile(TestUtils.toEntries(ids));
    }

    public static String newPartitionFile(OptOutEntry[] entries) {
        OptOutHeap heap = new OptOutHeap(1);
        try {
            heap.add(entries);
            Path tmpFile = Files.createTempFile(OptOutUtils.prefixPartitionFile, newSuffix());
            OptOutPartition s = heap.toPartition(true);
            Files.write(tmpFile, s.getStore());
            return tmpFile.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String newPartitionFile(String... logFiles) {
        OptOutHeap heap = new OptOutHeap(1);
        try {
            for (String logFile : logFiles) {
                byte[] data = Files.readAllBytes(Paths.get(logFile));
                heap.add(new OptOutCollection(data));
            }

            Path tmpFile = Files.createTempFile("optout-snapshot-", newSuffix());
            OptOutPartition s = heap.toPartition(true);
            Files.write(tmpFile, s.getStore());
            return tmpFile.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
