package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.shared.health.HealthComponent;
import com.uid2.shared.health.HealthManager;
import com.uid2.shared.optout.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

//
// consumes event:
//   - partition.produce (json logPaths)
//
// produces events:
//   - partition.produced (String filePath)
//
// partition is merge-sorted logs for a given larger time window (e.g. every 24hr)
//
public class OptOutLogProducer extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutLogProducer.class);
    private final HealthComponent healthComponent = HealthManager.instance.registerComponent("partition-producer");
    private final String partitionProducerDir;
    private final int replicaId;
    private final String eventPartitionProduced;
    private final FileUtils fileUtils;
    private Counter counterPartitionProduced = Counter
        .builder("uid2_optout_partition_produced_total")
        .description("counter for how many optout partition files are produced")
        .register(Metrics.globalRegistry);
    private WorkerExecutor partitionProducerExecutor = null;

    public OptOutLogProducer(JsonObject jsonConfig) {
        this(jsonConfig, Const.Event.PartitionProduced);
    }

    public OptOutLogProducer(JsonObject jsonConfig, String eventPartitionProduced) {
        this.healthComponent.setHealthStatus(false, "not started");

        this.partitionProducerDir = this.getPartitionProducerDir(jsonConfig);
        this.replicaId = OptOutUtils.getReplicaId(jsonConfig);
        LOGGER.info("replica id is set to " + this.replicaId);

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
            // create a special worker pool for partition producer
            this.partitionProducerExecutor = vertx.createSharedWorkerExecutor("partition-worker-pool");

            EventBus eb = vertx.eventBus();
            eb.<String>consumer(Const.Event.PartitionProduce, msg -> this.handlePartitionProduce(msg));

            this.mkdirsBlocking();
            startPromise.complete();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            startPromise.fail(new Throwable(ex));
        }

        startPromise.future()
            .onSuccess(v -> {
                LOGGER.info("started OptOutLogProducer");
                this.healthComponent.setHealthStatus(true);
            })
            .onFailure(e -> {
                LOGGER.error("failed starting OptOutLogProducer", e);
                this.healthComponent.setHealthStatus(false, e.getMessage());
            });
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        LOGGER.info("shutting down OptOutLogProducer.");
        if (this.partitionProducerExecutor != null) {
            this.partitionProducerExecutor.close();
        }
        stopPromise.complete();
        LOGGER.info("stopped OptOutLogProducer");
    }

    private void publishPartitionProducedEvent(String newPartition) {
        assert newPartition != null;
        this.counterPartitionProduced.increment();
        vertx.eventBus().publish(this.eventPartitionProduced, newPartition);
    }

    private void handlePartitionProduce(Message<String> msg) {
        // convert input string into array of delta files to combine into new partition
        String[] files = OptOutUtils.jsonArrayToStringArray(msg.body());

        // execute blocking operation using special worker-pool
        // when completed, publish partition.produced event
        this.partitionProducerExecutor.<String>executeBlocking(
            promise -> promise.complete(this.producePartitionBlocking(files)),
            res -> {
                if (res.succeeded()) {
                    this.publishPartitionProducedEvent(res.result());
                } else {
                    LOGGER.error("Failed to produce partition: " + res.cause().getMessage(), res.cause());
                }
            }
        );
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
        if (!fs.existsBlocking(this.partitionProducerDir)) {
            fs.mkdirsBlocking(this.partitionProducerDir);
        }
    }

    private void deleteExpiredLogsLocally() {
        deleteExpiredLogsLocally(this.partitionProducerDir);
    }

    private void deleteExpiredLogsLocally(String dirName) {
        if (!Files.exists(Paths.get(dirName))) return;

        Instant now = Instant.now();
        File dir = new File(dirName);
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File f : files) {
            if (fileUtils.isDeltaOrPartitionExpired(now, f.getName())) {
                try {
                    Files.delete(f.toPath());
                    LOGGER.warn("deleted expired log: " + f.getName());
                } catch (Exception e) {
                    LOGGER.error("Error deleting expired log: " + f.getName(), e);
                }
            }
        }
    }

    private String newPartitionFileName() {
        return Paths.get(this.partitionProducerDir, OptOutUtils.newPartitionFileName(this.replicaId)).toString();
    }

    public static String getPartitionProducerDir(JsonObject config) {
        return String.format("%s/producer/partition", config.getString(Const.Config.OptOutDataDirProp));
    }
}
