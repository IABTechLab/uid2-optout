package com.uid2.optout.vertx;

import com.uid2.optout.Const;
import com.uid2.optout.partner.EndpointConfig;
import com.uid2.optout.partner.IOptOutPartnerEndpoint;
import com.uid2.optout.partner.OptOutPartnerEndpoint;
import com.uid2.shared.health.HealthComponent;
import com.uid2.shared.health.HealthManager;
import com.uid2.shared.optout.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//
// OptOut sender service, one for each remote partner
//
// consumes event:
//   - cloudsync.optout.downloaded (String s3Path)
//
// produces events:
//   - delta.sent_remote (String <receive_name>,<comma_separated_filePaths>)
//
// take entries save in the delta file, and send to partner optout endpoints.
//
public class OptOutSender extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutSender.class);
    private final HealthComponent healthComponent;
    private final String deltaConsumerDir;
    private final int deltaRotateInterval;
    private final int replicaId;
    private final int senderReplicaId;
    private final int totalReplicas;
    private final IOptOutPartnerEndpoint remotePartner;
    private final Counter counterTotalEntriesSent;
    private final String eventCloudSyncDownloaded;
    private final AtomicInteger pendingFilesCount = new AtomicInteger(0);
    private final AtomicLong lastEndtrySent = new AtomicLong(Instant.EPOCH.getEpochSecond());
    private LinkedList<String> pendingFiles = new LinkedList<>();
    private boolean isReplaying = false;
    private CompletableFuture pendingAsyncOp = null;
    // name of the file that stores timestamp
    private Path timestampFile = null;
    // name of the file that stores processed deltas
    private Path processedDeltasFile = null;
    // timestamp when the last delta is processed
    private Instant lastProcessedTimestamp = null;

    public OptOutSender(JsonObject jsonConfig, Vertx vertx, EndpointConfig partnerConfig, String eventCloudDownloade) {
        this(jsonConfig, new OptOutPartnerEndpoint(vertx, partnerConfig), eventCloudDownloade);
    }

    public OptOutSender(JsonObject jsonConfig, IOptOutPartnerEndpoint optOutPartner, String eventCloudSyncDownloaded) {
        this.healthComponent = HealthManager.instance.registerComponent("optout-sender-" + optOutPartner.name());
        this.healthComponent.setHealthStatus(false, "not started");

        this.eventCloudSyncDownloaded = eventCloudSyncDownloaded;
        this.deltaConsumerDir = OptOutUtils.getDeltaConsumerDir(jsonConfig);
        this.replicaId = OptOutUtils.getReplicaId(jsonConfig);
        this.senderReplicaId = jsonConfig.getInteger(Const.Config.OptOutSenderReplicaIdProp);
        this.totalReplicas = jsonConfig.getInteger(Const.Config.OptOutProducerMaxReplicasProp);
        assert this.totalReplicas > 0;

        this.deltaRotateInterval = jsonConfig.getInteger(Const.Config.OptOutDeltaRotateIntervalProp);
        assert this.deltaRotateInterval > 0;

        this.remotePartner = optOutPartner;
        this.timestampFile = Paths.get(jsonConfig.getString(Const.Config.OptOutDataDirProp), "remote_replicate", this.remotePartner.name() + "_timestamp.txt");
        this.processedDeltasFile = Paths.get(jsonConfig.getString(Const.Config.OptOutDataDirProp), "remote_replicate", this.remotePartner.name() + "_processed.txt");

        Gauge.builder("uid2.optout.last_entry_sent", () -> this.lastEndtrySent.get())
            .description("gauge for last entry send epoch seconds, per each remote partner")
            .tag("remote_partner", remotePartner.name())
            .register(Metrics.globalRegistry);

        Gauge.builder("uid2.optout.pending_deltas_to_send", () -> this.pendingFilesCount.get())
            .description("gauge for remaining delta files to send to remote, per each remote partner")
            .tag("remote_partner", remotePartner.name())
            .register(Metrics.globalRegistry);

        this.counterTotalEntriesSent = Counter.builder("uid2.optout.entries_sent")
            .description("counter for total entries sent, per each remote partner")
            .tag("remote_partner", remotePartner.name())
            .register(Metrics.globalRegistry);
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        LOGGER.info("starting OptOutSender");
        this.healthComponent.setHealthStatus(false, "still starting");

        try {
            EventBus eb = vertx.eventBus();

            LOGGER.info("replica id is set to " + this.replicaId);
            if (this.replicaId == this.senderReplicaId) {
                LOGGER.info("this is replica " + this.replicaId + ", and will be responsible for consolidating deltas before replaying to remote");
                eb.<String>consumer(this.eventCloudSyncDownloaded, msg -> this.handleCloudDownloaded(msg));

                // before mark startPromise complete, scan local delta files and find unprocessed deltas
                this.scanLocalForUnprocessed().onComplete(ar -> startPromise.handle(ar));
            } else {
                LOGGER.info("this is not replica " + this.senderReplicaId + ", and will not be responsible for consolidating deltas before replaying to remote");
                startPromise.complete();
            }
        } catch (Exception ex) {
            LOGGER.fatal(ex.getMessage(), ex);
            startPromise.fail(new Throwable(ex));
        }

        startPromise.future()
            .onSuccess(v -> {
                LOGGER.info("started OptOutSender");
                this.healthComponent.setHealthStatus(true);
            })
            .onFailure(e -> {
                LOGGER.fatal("failed starting OptOutSender", e);
                this.healthComponent.setHealthStatus(false, e.getMessage());
            });
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        LOGGER.info("shutting down OptOutSender.");

        AtomicInteger shutdownTryCounter = new AtomicInteger(0);
        vertx.setPeriodic(500, i -> {
            if (this.isReplaying == false || shutdownTryCounter.incrementAndGet() > 120) {
                // wait for at most 60s (120 * 500ms) for current replaying to complete
                stopPromise.complete();
            }
        });

        stopPromise.future()
            .onSuccess(v -> LOGGER.info("stopped OptOutSender"))
            .onFailure(e -> LOGGER.fatal("failed stopping OptOutSender", e));
    }

    // returning name of the file that stores timestamp
    public Path getTimestampFile() {
        return this.timestampFile;
    }

    // returning name of the file that stores processed deltas
    public Path getProcessedDeltasFile() {
        return this.processedDeltasFile;
    }

    private Future<Void> scanLocalForUnprocessed() {
        Future step1 = OptOutUtils.readLinesFromFile(vertx, processedDeltasFile);
        Future step2 = OptOutUtils.readTimestampFromFile(vertx, timestampFile, 0);
        return CompositeFuture.all(step1, step2).compose(cf -> {
            HashSet<String> processedDeltas = new HashSet<>(Arrays.asList(cf.resultAt(0)));
            this.lastProcessedTimestamp = Instant.ofEpochSecond(cf.resultAt(1));
            LOGGER.info("found total " + processedDeltas.size() + " local deltas on disk");

            // checking our deltaConsumerDir
            File dirToList = new File(deltaConsumerDir);
            if (!dirToList.exists()) {
                return Future.succeededFuture();
            }

            String[] localFiles = null;
            try {
                localFiles = dirToList.list();
            } catch (Exception ex) {
                return Future.failedFuture(ex);
            }

            // localFiles can be null if deltaConsumerDir is not a directory
            if (localFiles == null) {
                return Future.succeededFuture();
            }

            // sort files
            Arrays.sort(localFiles, OptOutUtils.DeltaFilenameComparator);

            // enumerate files found and add them to pending files if they are not processed
            for (String f : localFiles) {
                if (!OptOutUtils.isDeltaFile(f)) continue;
                String fullName = Paths.get(deltaConsumerDir, f).toString();
                Instant fileTimestamp = OptOutUtils.getFileTimestamp(fullName);

                // regardless of timestamp, if a delta is unprocessed, adding it to the pending files
                if (!processedDeltas.contains(fullName)) {
                    // log an error if an unprocessed delta is found before the timestamp
                    if (fileTimestamp.isBefore(this.lastProcessedTimestamp)) {
                        LOGGER.error("unprocessed delta file: " + fullName + " found before the last processed timestamp: " + this.lastProcessedTimestamp);
                    }

                    this.pendingFiles.add(fullName);
                }
            }

            LOGGER.info("added " + this.pendingFiles.size() + " local deltas as pending deltas");

            return Future.succeededFuture();
        });
    }

    private void handleCloudDownloaded(Message<String> msg) {
        try {
            String filename = msg.body();
            if (!OptOutUtils.isDeltaFile(filename)) {
                LOGGER.info("ignoring non-delta file " + filename + " downloaded from s3");
                return;
            }

            LOGGER.info("received delta " + filename + " to consolidate and replicate to remote");
            OptOutUtils.addSorted(this.pendingFiles, filename, OptOutUtils.DeltaFilenameComparator);

            // if it is still replaying the last one, return
            if (this.isReplaying) return;

            this.processPendingFilesToConsolidate(Instant.now());
        } catch (Exception ex) {
            LOGGER.fatal("handleLogReplay failed unexpectedly: " + ex.getMessage(), ex);
        }
    }

    private void processPendingFilesToConsolidate(Instant now) {
        Instant currentSlot = OptOutUtils.instantFloorByInterval(now, this.deltaRotateInterval);

        this.pendingFilesCount.set(this.pendingFiles.size());

        // short-circuit if there are no pending files
        if (this.pendingFiles.size() == 0) return;

        // process pending files in sorted order (from earlier to later)
        int deltasForCurrentIntervalReceived = 0;
        List<String> deltasToConsolidate = new ArrayList<String>();
        Instant nextTimestamp = null;
        if (this.lastProcessedTimestamp == Instant.EPOCH) {
            // if lastProcessedTimestamp is not initialized, just process up to the current timestamp
            Instant firstDeltaTimestamp = OptOutUtils.getFileTimestamp(this.pendingFiles.get(0));
            nextTimestamp = firstDeltaTimestamp.plusSeconds(OptOutUtils.getSecondsBeforeNextSlot(firstDeltaTimestamp, this.deltaRotateInterval));
            LOGGER.info("last processed timestamp is found to be uninitialized, will process all deltas up to: " + nextTimestamp);
        } else {
            nextTimestamp = this.lastProcessedTimestamp.plus(this.deltaRotateInterval, ChronoUnit.SECONDS);
            LOGGER.info("last processed timestamp is " + this.lastProcessedTimestamp + ", will process all deltas up to: " + nextTimestamp);
        }

        ListIterator<String> iterator = this.pendingFiles.listIterator();
        while (iterator.hasNext()) {
            String f = iterator.next();
            Instant fileTimestamp = OptOutUtils.getFileTimestamp(f);
            if (fileTimestamp.getEpochSecond() >= nextTimestamp.getEpochSecond()) {
                // pending files list is sorted, so stop processing if found file later than the timestamp
                break;
            }

            if (fileTimestamp.getEpochSecond() >= this.lastProcessedTimestamp.getEpochSecond()) {
                // count how many files received for the intended consolidating time window
                ++deltasForCurrentIntervalReceived;
            }

            // adding file to deltas to consolidate
            deltasToConsolidate.add(f);
        }

        // either we received deltas from all replicas, or we waited for an entire delta rotation interval
        LOGGER.info("current slot: " + currentSlot);
        if (deltasForCurrentIntervalReceived >= this.totalReplicas || currentSlot.isAfter(nextTimestamp)) {
            if (deltasToConsolidate.size() == 0) {
                LOGGER.info("received 0 new deltas, between " + this.lastProcessedTimestamp + " and " + nextTimestamp);
            } else {
                LOGGER.info("received " + deltasForCurrentIntervalReceived + " new deltas, and total of " + deltasToConsolidate.size() + " to consolidate between " + this.lastProcessedTimestamp + " and " + nextTimestamp);
            }

            // if received deltas is the same or greater than the total replicas in the current consolidating window
            // or if the consolidating time window (last, last + deltaRotateInterval) is already in the past
            this.isReplaying = true;
            this.kickOffDeltaReplayWithConsolidation(nextTimestamp, deltasToConsolidate);
        }
    }

    private void kickOffDeltaReplayWithConsolidation(Instant nextTimestamp, List<String> deltasToConsolidate) {
        vertx.<Void>executeBlocking(promise -> deltaReplayWithConsolidation(promise, deltasToConsolidate),
            ar -> {
                if (ar.failed()) {
                    LOGGER.fatal("delta consolidation failed", new Exception(ar.cause()));
                } else {
                    updateProcessedDeltas(nextTimestamp, deltasToConsolidate);
                    // once complete, check if we could start the next round
                    this.lastProcessedTimestamp = nextTimestamp;
                }

                // call process again
                this.isReplaying = false;
                this.processPendingFilesToConsolidate(Instant.now());
            }
        );
    }

    private void updateProcessedDeltas(Instant nextTimestamp, List<String> deltasConsolidated) {
        if (deltasConsolidated.size() == 0) {
            LOGGER.info("skip updating processed delta timestamp due to 0 deltas being processed");
            return;
        }

        LOGGER.info("updating processed delta timestamp to: " + nextTimestamp);
        OptOutUtils.writeTimestampToFile(vertx, this.timestampFile, nextTimestamp.getEpochSecond()).compose(v -> {
            LOGGER.info("updated processed delta timestamp to: " + nextTimestamp);

            // if no files in the list, skip appending process delta filenames to disk
            if (deltasConsolidated.size() == 0) return Future.succeededFuture();

            // persist the list of files on disk
            LOGGER.info("appending " + deltasConsolidated.size() + " files to processed delta list");
            return OptOutUtils.appendLinesToFile(vertx, this.processedDeltasFile, deltasConsolidated);
        }).onFailure(v -> {
            String filenames = String.join(",", deltasConsolidated);
            LOGGER.fatal("unable to persistent last delta timestamp and/or processed delta filenames: " + nextTimestamp + ": " + filenames);
        });

        for (String deltaFile : deltasConsolidated) {
            this.pendingFiles.remove(deltaFile);
        }

        LOGGER.info("removed " + deltasConsolidated.size() + " delta(s) from pending to process list");
    }

    private void deltaReplayWithConsolidation(Promise<Void> promise, List<String> deltasToConsolidate) {
        if (deltasToConsolidate.size() == 0) {
            // if no files in the list, short-circuit and complete the promise
            promise.complete();
            return;
        }

        try {
            OptOutHeap heap = new OptOutHeap(1000);
            for (String deltaFile : deltasToConsolidate) {
                LOGGER.info("loading delta " + deltaFile);
                Path fp = Paths.get(deltaFile);

                try {
                    byte[] data = Files.readAllBytes(fp);
                    OptOutCollection store = new OptOutCollection(data);
                    heap.add(store);
                } catch (NoSuchFileException ex) {
                    LOGGER.error("ignoring non-existing file: " + ex.getFile().toString());
                }
            }

            OptOutPartition consolidatedDelta = heap.toPartition(true);
            deltaReplay(promise, consolidatedDelta, deltasToConsolidate);
        } catch (Exception ex) {
            LOGGER.fatal("deltaReplay failed unexpectedly: " + ex.getMessage(), ex);
            // this error is a code logic error and needs to be fixed
            promise.fail(new Throwable(ex));
        }
    }

    private void deltaReplay(Promise<Void> promise, OptOutCollection store, List<String> fileList) {
        try {
            // generate comma separated filename list for logging
            String filenames = String.join(",", fileList);
            this.pendingAsyncOp = new CompletableFuture();

            // sequentially send each entry
            Future<Void> lastOp = Future.succeededFuture();
            for (int i = 0; i < store.size(); ++i) {
                final OptOutEntry entry = store.get(i);
                lastOp = lastOp.compose(v -> this.remotePartner.send(entry));
                lastOp.onComplete(v -> {
                    this.lastEndtrySent.set(Instant.now().getEpochSecond());
                    this.counterTotalEntriesSent.increment();
                });
            }

            lastOp.onComplete(ar -> {
                if (ar.failed()) {
                    LOGGER.fatal("deltaReplay failed sending delta " + filenames + " to remote: " + this.remotePartner.name(), ar.cause());
                    LOGGER.error("deltaReplay has " + this.pendingFilesCount.get() + " pending file");
                    LOGGER.error("deltaReplay will restart in 3600s");
                    vertx.setTimer(1000 * 3600, i -> this.pendingAsyncOp.completeExceptionally(new Exception(ar.cause())));
                } else {
                    LOGGER.info("finished delta replay for file: " + filenames);
                    this.pendingAsyncOp.complete(null);

                    String completeMsg = this.remotePartner.name() + "," + filenames;
                    vertx.eventBus().send(Const.Event.DeltaSentRemote, completeMsg);
                }
            });

            // this causes it to block on the worker thread (this function should be called on a worker thread)
            try {
                this.pendingAsyncOp.get();
                promise.complete();
            } catch (Exception ex) {
                LOGGER.fatal(ex);
                promise.fail(ex);
            } finally {
                this.pendingAsyncOp = null;
            }
        } catch (Exception ex) {
            LOGGER.fatal("deltaReplay failed unexpectedly: " + ex.getMessage(), ex);
            // this error is a code logic error and needs to be fixed
            promise.fail(new Throwable(ex));
        }
    }
}
