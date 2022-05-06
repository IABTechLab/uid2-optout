// Copyright (c) 2021 The Trade Desk, Inc
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
//    this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.shared.health.HealthComponent;
import com.uid2.shared.health.HealthManager;
import com.uid2.shared.optout.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

//
// consumes event:
//   - entry.add (string identityHash,advertisingId)
//   - delta.produce (json logPaths)
//   - partition.produce (json logPaths)
//
// produces events:
//   - delta.produced (String filePath)
//   - partition.produced (String filePath)
//
// there are 2 types of optout log file: delta and partition
// delta is raw optout log file produced at a regular cadence (e.g. 5 mins)
// partition is merge-sorted logs for a given larger time window (e.g. every 24hr)
//
public class OptOutLogProducer extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutLogProducer.class);
    private final HealthComponent healthComponent = HealthManager.instance.registerComponent("log-producer");
    private final String deltaProducerDir;
    private final String partitionProducerDir;
    private final int replicaId;
    private final ArrayList<Message<String>> bufferedMessages;
    private final String eventDeltaProduced;
    private final String eventPartitionProduced;
    private final FileUtils fileUtils;
    private Counter counterDeltaProduced = Counter
        .builder("uid2.optout.delta_produced")
        .description("counter for how many optout delta files are produced")
        .register(Metrics.globalRegistry);
    private Counter counterPartitionProduced = Counter
        .builder("uid2.optout.partition_produced")
        .description("counter for how many optout partition files are produced")
        .register(Metrics.globalRegistry);
    private ByteBuffer buffer;
    private String currentDeltaFileName = null;
    private boolean writeInProgress = false;
    private boolean shutdownInProgress = false;
    private FileChannel fileChannel = null;
    private WorkerExecutor partitionProducerExecutor = null;
    private int writeErrorsSinceDeltaOpen = 0;

    public OptOutLogProducer(JsonObject jsonConfig) throws IOException {
        this(jsonConfig, Const.Event.DeltaProduced, Const.Event.PartitionProduced);
    }

    public OptOutLogProducer(JsonObject jsonConfig, String eventDeltaProduced, String eventPartitionProduced) throws IOException {
        this.healthComponent.setHealthStatus(false, "not started");

        this.deltaProducerDir = this.getDeltaProducerDir(jsonConfig);
        this.partitionProducerDir = this.getPartitionProducerDir(jsonConfig);
        this.replicaId = OptOutUtils.getReplicaId(jsonConfig);
        LOGGER.info("replica id is set to " + this.replicaId);

        int bufferSize = jsonConfig.getInteger(Const.Config.OptOutProducerBufferSizeProp);
        this.buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
        this.bufferedMessages = new ArrayList<Message<String>>();

        this.eventDeltaProduced = eventDeltaProduced;
        this.eventPartitionProduced = eventPartitionProduced;

        this.fileUtils = new FileUtils(jsonConfig);
        
        if (jsonConfig.getBoolean(Const.Config.OptOutDeleteExpiredProp)) {
            this.deleteExpiredLogsLocally();
        }
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        LOGGER.info("starting OptOutLogProducer");
        this.healthComponent.setHealthStatus(false, "still starting");

        try {
            // create a special worker pool for partition producer, so that it doesn't block log producer
            this.partitionProducerExecutor = vertx.createSharedWorkerExecutor("partition-worker-pool");

            EventBus eb = vertx.eventBus();
            eb.<String>consumer(Const.Event.EntryAdd, msg -> this.handleEntryAdd(msg));
            eb.consumer(Const.Event.DeltaProduce, msg -> this.handleDeltaProduce(msg));
            eb.<String>consumer(Const.Event.PartitionProduce, msg -> this.handlePartitionProduce(msg));

            // start delta rotating
            this.deltaRotate(false).onComplete(
                ar -> startPromise.handle(ar));
        } catch (Exception ex) {
            LOGGER.fatal(ex.getMessage(), ex);
            startPromise.fail(new Throwable(ex));
        }

        startPromise.future()
            .onSuccess(v -> {
                LOGGER.info("started OptOutLogProducer");
                this.healthComponent.setHealthStatus(true);
            })
            .onFailure(e -> {
                LOGGER.fatal("failed starting OptOutLogProducer", e);
                this.healthComponent.setHealthStatus(false, e.getMessage());
            });
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        LOGGER.info("shutting down OptOutLogProducer.");
        this.deltaRotate(true).onComplete(
            ar -> stopPromise.handle(ar));
        stopPromise.future()
            .onSuccess(v -> LOGGER.info("stopped OptOutLogProducer"))
            .onFailure(e -> LOGGER.fatal("failed stopping OptOutLogProducer", e));
    }

    public String getLastDelta() {
        String[] deltaList = (new File(this.deltaProducerDir)).list();
        if (deltaList == null) return null;
        Optional<String> last = Arrays.stream(deltaList)
            .sorted(OptOutUtils.DeltaFilenameComparatorDescending)
            .findFirst();
        if (last.isPresent()) return Paths.get(this.deltaProducerDir, last.get()).toString();
        return null;
    }

    private Future<Void> deltaRotate(boolean shuttingDown) {
        Promise<Void> promise = Promise.promise();
        vertx.<Void>executeBlocking(
            blockingPromise -> {
                try {
                    String newDelta = this.deltaRotateBlocking(shuttingDown);
                    if (newDelta != null) this.publishDeltaProducedEvent(newDelta);
                    ;
                    blockingPromise.complete();
                } catch (Exception ex) {
                    LOGGER.fatal(ex.getMessage(), ex);
                    blockingPromise.fail(new Throwable(ex));
                }
            },
            res -> promise.handle(res)
        );
        return promise.future();
    }

    private void publishDeltaProducedEvent(String newDelta) {
        assert newDelta != null;
        this.counterDeltaProduced.increment();
        vertx.eventBus().publish(this.eventDeltaProduced, newDelta);
    }

    private void publishPartitionProducedEvent(String newPartition) {
        assert newPartition != null;
        this.counterPartitionProduced.increment();
        vertx.eventBus().publish(this.eventPartitionProduced, newPartition);
    }

    private void handleEntryAdd(Message<String> entryMsg) {
        if (this.shutdownInProgress) {
            // if this event is received after shutdownInProgress is set, there is no file to write to at this point
            entryMsg.reply(false);
            return;
        }

        String body = entryMsg.body();
        if (!body.contains(",")) {
            LOGGER.fatal("unexpected optout entry format: " + body);
            // fast fail if the message doesn't contain a comma (identity_hash,advertising_id)
            entryMsg.reply(false);
            return;
        }

        String[] parts = body.split(",");
        if (parts.length != 3) {
            LOGGER.fatal("unexpected optout entry format: " + body);
            // fast fail if the message doesn't contain a comma (identity_hash,advertising_id)
            entryMsg.reply(false);
            return;
        }

        byte[] identityHash = OptOutUtils.base64StringTobyteArray(parts[0]);
        if (identityHash == null) {
            LOGGER.fatal("unexpected optout identity_hash: " + parts[0]);
            // fast fail if the message doesn't contain a valid identity_hash
            entryMsg.reply(false);
            return;
        }

        byte[] advertisingId = OptOutUtils.base64StringTobyteArray(parts[1]);
        if (advertisingId == null) {
            LOGGER.fatal("unexpected optout identity_hash: " + parts[1]);
            // fast fail if the message doesn't contain a valid advertising_id
            entryMsg.reply(false);
            return;
        }

        long timestampEpoch = -1;
        try {
            timestampEpoch = Long.valueOf(parts[2]);
        } catch (NumberFormatException e) {
            LOGGER.fatal("unexpected optout timestamp: " + parts[2]);
            // fast fail if the message doesn't contain a valid unix epoch timestamp for optout entry
            entryMsg.reply(false);
            return;
        }

        // add current msg to buffer
        bufferedMessages.add(entryMsg);

        // if there are no blocking write in progress, start one
        if (!this.writeInProgress) {
            this.writeInProgress = true;
            this.kickoffWrite();
        }
    }

    private void kickoffWrite() {
        // current msg will be written as a batch
        ArrayList<Message> batch = new ArrayList<Message>(bufferedMessages);
        bufferedMessages.clear();

        vertx.executeBlocking(
            promise -> {
                this.writeLogBlocking(batch);
                promise.complete();
            },
            res -> kickoffWriteCallback()
        );
    }

    private void kickoffWriteCallback() {
        if (bufferedMessages.size() > 0)
            kickoffWrite();
        else
            this.writeInProgress = false;
    }

    private void handleDeltaProduce(Message m) {
        vertx.<String>executeBlocking(
            promise -> promise.complete(this.deltaRotateBlocking(false)),
            res -> this.publishDeltaProducedEvent(res.result())
        );
    }

    private void handlePartitionProduce(Message<String> msg) {
        // convert input string into array of delta files to combine into new partition
        String[] files = OptOutUtils.jsonArrayToStringArray(msg.body());

        // execute blocking operation using special worker-pool
        // when completed, publish partition.produced event
        this.partitionProducerExecutor.<String>executeBlocking(
            promise -> promise.complete(this.producePartitionBlocking(files)),
            res -> this.publishPartitionProducedEvent(res.result())
        );
    }

    // this function is no-throw
    private void writeLogBlocking(ArrayList<Message> batch) {
        if (this.shutdownInProgress) {
            // if flag is set, file is already closed and we can't write any more due to verticle being shutdown
            assert this.fileChannel == null;
            for (Message<String> m : batch) {
                m.reply(false);
            }
            return;
        }

        try {
            assert this.fileChannel != null;

            // make sure buffer size is large enough
            this.checkBufferSize(batch.size() * OptOutConst.EntrySize);

            // write optout entries
            for (Message<String> m : batch) {
                String body = m.body();
                String[] parts = body.split(",");
                assert parts.length == 3;
                byte[] identityHash = OptOutUtils.base64StringTobyteArray(parts[0]);
                byte[] advertisingId = OptOutUtils.base64StringTobyteArray(parts[1]);
                long timestamp = Long.valueOf(parts[2]);
                assert identityHash != null && advertisingId != null;

                OptOutEntry.writeTo(buffer, identityHash, advertisingId, timestamp);
            }
            buffer.flip();
            this.fileChannel.write(buffer);
        } catch (Exception ex) {
            LOGGER.fatal("write delta failed: " + ex.getMessage(), ex);
            // report unhealthy status
            ++this.writeErrorsSinceDeltaOpen;

            // if file write failed, reply false with original messages for entry.add
            for (Message<String> m : batch) {
                m.reply(false);
            }
            return;
        } finally {
            // clearing the buffer
            buffer.clear();
        }

        // on success, reply true with original messages
        for (Message<String> m : batch) {
            m.reply(true);
        }
    }

    private void checkBufferSize(int dataSize) {
        ByteBuffer b = this.buffer;
        if (b.capacity() < dataSize) {
            int newCapacity = Integer.highestOneBit(dataSize) << 1;
            LOGGER.warn("Expanding buffer size: current " + b.capacity() + ", need " + dataSize + ", new " + newCapacity);
            this.buffer = ByteBuffer.allocate(newCapacity).order(ByteOrder.LITTLE_ENDIAN);
        }
    }

    // this function is no-throw
    private String deltaRotateBlocking(boolean shuttingDown) {
        this.mkdirsBlocking();
        String logProduced = null;

        // close current delta file if needed
        if (this.fileChannel != null) {
            logProduced = this.currentDeltaFileName;
            assert logProduced != null;

            // add a special last entry with ffff hash and the current timestamp
            buffer.put(OptOutUtils.onesHashBytes);
            buffer.put(OptOutUtils.onesHashBytes);
            buffer.putLong(OptOutUtils.nowEpochSeconds());
            try {
                buffer.flip();
                this.fileChannel.write(buffer);
            } catch (Exception ex) {
                // report unhealthy status
                ++this.writeErrorsSinceDeltaOpen;
                LOGGER.fatal("write last entry to delta failed: " + ex.getMessage(), ex);
                assert false;
            } finally {
                buffer.clear();
            }

            // save old delta file
            try {
                this.fileChannel.close();
                this.fileChannel = null;
            } catch (Exception ex) {
                // report unhealthy status
                ++this.writeErrorsSinceDeltaOpen;
                LOGGER.fatal("close delta file failed: " + ex.getMessage(), ex);
                assert false;
            }
        }

        // create a new file if not shutting down
        if (!shuttingDown) {
            // create new delta file
            Path logPath = Paths.get(this.newDeltaFileName());
            try {
                this.fileChannel = FileChannel.open(logPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, StandardOpenOption.SYNC);
                this.currentDeltaFileName = logPath.toString();
            } catch (Exception ex) {
                // report unhealthy status
                ++this.writeErrorsSinceDeltaOpen;
                LOGGER.fatal("open delta file failed" + ex.getMessage(), ex);
                assert false;
            }

            // asserting buffer is clear
            assert this.buffer.position() == 0;

            // add a special first entry with null hash and the current timestamp
            buffer.put(OptOutUtils.nullHashBytes);
            buffer.put(OptOutUtils.nullHashBytes);
            buffer.putLong(OptOutUtils.nowEpochSeconds());
            try {
                buffer.flip();
                this.fileChannel.write(buffer);
            } catch (Exception ex) {
                // report unhealthy status
                ++this.writeErrorsSinceDeltaOpen;
                LOGGER.fatal("write first entry to delta failed: " + ex.getMessage(), ex);
                assert false;
            } finally {
                buffer.clear();
            }

            // reset isHealthy status when a new file is open
            this.writeErrorsSinceDeltaOpen = 0;
        }

        if (shuttingDown) {
            // there is a race condition, between:
            // a) the delta being rotated for shutting down
            // b) the verticle being shutdown
            // and set the flag here to fail those entry.add requests that are received in between
            this.shutdownInProgress = true;
        }

        return logProduced;
    }

    // this function is no throw
    private String producePartitionBlocking(String[] logFiles) {
        // load list of files into optout heap
        OptOutHeap heap = new OptOutHeap(1);
        try {
            for (String file : logFiles) {
                Path filePath = Paths.get(file);
                byte[] data = Files.readAllBytes(filePath);
                heap.add(new OptOutCollection(data));
            }
        } catch (Exception ex) {
            // TODO: read error on partition producing isn't tolerated
            LOGGER.error(ex.getMessage(), ex);
            assert false;
        }

        // sort and write partition file
        String newPartitionFile = this.newPartitionFileName();
        try {
            Path newPath = Paths.get(newPartitionFile);
            OptOutPartition s = heap.toPartition(true);
            Files.write(newPath, s.getStore());
        } catch (Exception ex) {
            // TODO: write error on partition producing isn't tolerated
            LOGGER.error(ex.getMessage(), ex);
            assert false;
        }

        // fulfill promise with new partition filename
        return newPartitionFile;
    }

    private void mkdirsBlocking() {
        FileSystem fs = vertx.fileSystem();
        if (!fs.existsBlocking(this.deltaProducerDir)) {
            fs.mkdirsBlocking(this.deltaProducerDir);
        }
        if (!fs.existsBlocking(this.partitionProducerDir)) {
            fs.mkdirsBlocking(this.partitionProducerDir);
        }
    }


    private void deleteExpiredLogsLocally() throws IOException {
        deleteExpiredLogsLocally(this.deltaProducerDir);
        deleteExpiredLogsLocally(this.partitionProducerDir);
    }

    private void deleteExpiredLogsLocally(String dirName) throws IOException {
        if (!Files.exists(Paths.get(dirName))) return;

        Instant now = Instant.now();
        File dir = new File(dirName);
        File[] files = dir.listFiles();
        for (File f : files) {
            if (fileUtils.isDeltaOrPartitionExpired(now, f.getName())) {
                Files.delete(f.toPath());
                LOGGER.warn("deleted expired log: " + f.getName());
            }
        }
    }

    private String newDeltaFileName() {
        return Paths.get(this.deltaProducerDir, OptOutUtils.newDeltaFileName(this.replicaId)).toString();
    }

    private String newPartitionFileName() {
        return Paths.get(this.partitionProducerDir, OptOutUtils.newPartitionFileName(this.replicaId)).toString();
    }

    public static String getDeltaProducerDir(JsonObject config) {
        return String.format("%s/producer/delta", config.getString(Const.Config.OptOutDataDirProp));
    }

    public static String getPartitionProducerDir(JsonObject config) {
        return String.format("%s/producer/partition", config.getString(Const.Config.OptOutDataDirProp));
    }
}
