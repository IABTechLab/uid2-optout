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

package com.uid2.optout.tool;

import com.uid2.optout.Const;
import com.uid2.shared.Utils;
import com.uid2.shared.cloud.*;
import com.uid2.shared.optout.*;
import com.uid2.shared.vertx.VertxUtils;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.cli.Argument;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class OptOutLogTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(OptOutLogTool.class);

    private final ICloudStorage fsLocal = new LocalStorageMock();
    private JsonObject config;
    private boolean isVerbose = false;
    private boolean isYes = false;

    OptOutLogTool(JsonObject config) {
        this.config = config;
    }

    public static void main(String[] args) throws Exception {
        VertxOptions options = new VertxOptions()
            .setBlockedThreadCheckInterval(60*60*1000);
        Vertx vertx = Vertx.vertx(options);
        final String vertxConfigPath = System.getProperty(Const.Config.VERTX_CONFIG_PATH_PROP);
        if (vertxConfigPath != null) {
            System.out.format("Running CUSTOM CONFIG mode, config: %s\n", vertxConfigPath);
        }
        else if (!Utils.isProductionEnvionment()) {
            System.out.format("Running LOCAL DEBUG mode, config: %s\n", Const.Config.LOCAL_CONFIG_PATH);
            System.setProperty(Const.Config.VERTX_CONFIG_PATH_PROP, Const.Config.LOCAL_CONFIG_PATH);
        } else {
            System.out.format("Running PRODUCTION mode, config: %s\n", Const.Config.OVERRIDE_CONFIG_PATH);
        }

        VertxUtils.createConfigRetriever(vertx).getConfig(ar -> {
            OptOutLogTool tool = new OptOutLogTool(ar.result());
            try {
                tool.run(args);
            } catch (Exception e) {
                e.printStackTrace();
                vertx.close();
                System.exit(1);
            } finally {
                vertx.close();
            }
        });
    }

    public void run(String[] args) throws Exception {
        CommandLine cli = parseArgs(args);
        this.isVerbose = cli.isFlagEnabled("verbose");
        if (this.isVerbose) {
            LOGGER.info("VERBOSE on");
        }
        this.isYes = cli.isFlagEnabled("yes");
        if (this.isYes) {
            LOGGER.info("Pre-confirmed to proceed with potentially DESTRUCTIVE operation...");
        }

        Set<String> supportedCommands = new HashSet<>();
        supportedCommands.add("sync");
        supportedCommands.add("pack");
        supportedCommands.add("push");
        supportedCommands.add("verify");
        supportedCommands.add("dump");
        supportedCommands.add("generate");
        supportedCommands.add("purge");
        supportedCommands.add("gc");
        supportedCommands.add("cronjob");

        String command = cli.getArgumentValue("command");
        if (!supportedCommands.contains(command)) {
            System.err.println("Unknown command: " + command);
        } else if ("sync".equals(command)) {
            String dataDir = cli.getOptionValue("datadir");
            checkOptionExistence(dataDir, command, "datadir");
            runSync(dataDir);
        } else if ("pack".equals(command)) {
            String dataDir = cli.getOptionValue("datadir");
            checkOptionExistence(dataDir, command, "datadir");
            String outDir = cli.getOptionValue("outdir");
            checkOptionExistence(outDir, command, "outdir");
            String packedFile = runPack(dataDir, outDir, cli.isFlagEnabled("cutoffutc"));
            if (packedFile == null) System.exit(1);
        } else if ("dump".equals(command)) {
            String dataDir = cli.getOptionValue("datadir");
            checkOptionExistence(dataDir, command, "datadir");
            String id = cli.getOptionValue("id");
            checkOptionExistence(id, command, "id");
            runDump(dataDir, id);
        } else if ("push".equals(command)) {
            String file = cli.getOptionValue("file");
            checkOptionExistence(file, command, "file");
            runPush(file);
        } else if ("verify".equals(command)) {
            String file = cli.getOptionValue("file");
            checkOptionExistence(file, command, "file");
            String url = cli.getOptionValue("url");
            checkOptionExistence(url, command, "url");
            runVerify(file, url);
        } else if ("generate".equals(command)) {
            String outDir = cli.getOptionValue("outdir");
            checkOptionExistence(outDir, command, "outdir");
            String salt = cli.getOptionValue("salt");
            checkOptionExistence(salt, command, "salt");
            int count = Integer.valueOf(cli.getOptionValue("count"));
            runGenerate(outDir, salt, count);
        } else if ("gc".equals(command)) {
            runGc();
        } else if ("purge".equals(command)) {
            String dataDir = cli.getOptionValue("datadir");
            checkOptionExistence(dataDir, command, "datadir");
            runPurge(dataDir);
        } else if ("cronjob".equals(command)) {
            runCronJob(cli.isFlagEnabled("pack"));
        }
    }

    private void checkOptionExistence(String val, String command, String option) throws Exception {
        if (val == null) {
            throw new Exception("option -" + option + " is required for command " + command);
        }
    }

    private CommandLine parseArgs(String[] args) {
        final CLI cli = CLI.create("optout-log-tool")
            .setSummary("A tool for managing optout log files")
            .addArgument(new Argument()
                .setArgName("command")
                .setDescription("command to run, can be one of: sync, pack, push, verify, generate, dump, gc, purge, cronjob")
                .setRequired(true))
            .addOption(new Option()
                .setLongName("datadir")
                .setShortName("d")
                .setDescription("path to data directory")
                .setRequired(false))
            .addOption(new Option()
                .setLongName("outdir")
                .setShortName("o")
                .setDescription("path to output directory")
                .setRequired(false))
            .addOption(new Option()
                .setLongName("id")
                .setShortName("i")
                .setDescription("identity to dump")
                .setRequired(false))
            .addOption(new Option()
                .setLongName("file")
                .setShortName("f")
                .setDescription("path to file to push to cloud storage")
                .setRequired(false))
            .addOption(new Option()
                .setLongName("url")
                .setShortName("u")
                .setDescription("optout api url to verify timestamp")
                .setRequired(false))
            .addOption(new Option()
                .setLongName("salt")
                .setShortName("s")
                .setDescription("first level salt for generate first level email hash")
                .setRequired(false))
            .addOption(new Option()
                .setLongName("count")
                .setShortName("n")
                .setDescription("number of entries to generate (1MM entries per partition file)")
                .setRequired(false))
            .addOption(new Option()
                .setLongName("cutoffutc")
                .setShortName("cu")
                .setDescription("cut off at utc 00, skip logs after the cutoff for compacting")
                .setFlag(true)
                .setRequired(false))
            .addOption(new Option()
                .setLongName("pack")
                .setShortName("p")
                .setDescription("enable packing (merge all existing files into one) in cronjob")
                .setFlag(true)
                .setRequired(false))
            .addOption(new Option()
                .setLongName("verbose")
                .setShortName("v")
                .setDescription("allow verbose logging")
                .setFlag(true)
                .setRequired(false))
            .addOption(new Option()
                .setLongName("yes")
                .setShortName("y")
                .setDescription("confirm to proceed with operation")
                .setFlag(true)
                .setRequired(false));
        return cli.parse(Arrays.asList(args));
    }

    private ICloudStorage wrapCloudStorageForOptOut(ICloudStorage cloudStorage) {
        if (config.getBoolean(Const.Config.OptOutS3PathCompatProp)) {
            // LOGGER.warn("Using S3 Path Compatibility Conversion: log -> delta, snapshot -> partition");
            return new PathConversionWrapper(
                cloudStorage,
                in -> {
                    String out = in.replace("log", "delta")
                        .replace("snapshot", "partition");
                    // LOGGER.debug("S3 path forward convert: " + in + " -> " + out);
                    return out;
                },
                in -> {
                    String out = in.replace("delta", "log")
                        .replace("partition", "snapshot");
                    // LOGGER.debug("S3 path backward convert: " + in + " -> " + out);
                    return out;
                }
            );
        } else {
            return cloudStorage;
        }
    }

    private void runSync(String dataDir) throws CloudStorageException {
        dataDir = Paths.get(dataDir).toAbsolutePath().toString();
        Utils.ensureDirectoryExists(dataDir);

        JsonObject mergedConfig = new JsonObject().mergeIn(config)
            .put(Const.Config.OptOutDataDirProp, dataDir);
        this.config = mergedConfig;

        OptOutCloudSync cloudSync = new OptOutCloudSync(mergedConfig, true);
        String bucket = mergedConfig.getString(Const.Config.OptOutS3BucketProp);
        ICloudStorage cloudStorage = wrapCloudStorageForOptOut(CloudUtils.createStorage(bucket, mergedConfig));
        List<Exception> exceptionList = new ArrayList<>();
        cloudSync.refresh(Instant.now(),
            cloudStorage,
            fsLocal,
            downloads -> {
                List<String> sortedDownload = downloads.stream()
                    .sorted(OptOutUtils.DeltaFilenameComparator)
                    .collect(Collectors.toList());
                for (String d : sortedDownload) {
                    String l = cloudSync.toLocalPath(d);
                    try {
                        Files.copy(cloudStorage.download(d), Paths.get(l), StandardCopyOption.REPLACE_EXISTING);
                        LOGGER.trace("Saved " + d + " to " + l);
                    } catch (Exception e) {
                        System.err.println("Unable to download " + d + ": " + e.getMessage());
                        e.printStackTrace(System.err);
                        exceptionList.add(e);
                    }
                }
            },
            deletes -> {
                for (String d : deletes) {
                    try {
                        Files.delete(Paths.get(d));
                        LOGGER.info("Deleted " + d);
                    } catch (IOException e) {
                        System.err.println("Unable to delete " + d + ": " + e.getMessage());
                        e.printStackTrace(System.err);
                        exceptionList.add(e);
                    }
                }
            });

        if (exceptionList.size() > 0) {
            System.err.println("Total " + exceptionList.size() + " errors during sync");
            System.exit(1);
        }
    }

    private String runPack(String dataDir, String outDir, boolean cutOffUtc) throws IOException {
        JsonObject mergedConfig = new JsonObject().mergeIn(config)
            .put(Const.Config.OptOutDataDirProp, dataDir);
        this.config = mergedConfig;

        outDir = Paths.get(outDir).toAbsolutePath().toString();
        Utils.ensureDirectoryExists(outDir);

        HashSet<String> deltas = new HashSet<>();
        HashSet<String> partitions = new HashSet<>();

        // pack up to yesterday
        Instant deadline = cutOffUtc
            ? Instant.now().truncatedTo(ChronoUnit.DAYS).minusSeconds(1)
            : Instant.now();
        LOGGER.info("Pack optout entries found in partitions before: " + deadline);

        String partitionDir = OptOutUtils.getPartitionConsumerDir(config);
        LOGGER.info("Packing partition dir: " + partitionDir);
        HashMap<String, String> idLastFoundPartition = new HashMap<>();
        String packedPartitionOutFile = packDir(partitionDir, outDir, idLastFoundPartition, partitions, deadline);
        LOGGER.info("Partitions packed into: " + packedPartitionOutFile);

        Instant lastSnap = cutOffUtc
            ? OptOutUtils.lastPartitionTimestamp(partitions)
            : deadline;

        String deltaDir = OptOutUtils.getDeltaConsumerDir(config);
        LOGGER.info("Packing delta dir: " + deltaDir);
        HashMap<String, String> idLastFoundDelta = new HashMap<>();
        String packedDeltaOutFile = packDir(deltaDir, outDir, idLastFoundDelta, deltas, lastSnap);
        LOGGER.info("Deltas packed into: " + packedDeltaOutFile);

        boolean inconsistencyFound = false;
        HashSet<String> missingIds1 = new HashSet<>(idLastFoundDelta.keySet());
        missingIds1.removeAll(idLastFoundPartition.keySet());
        for (String id : missingIds1) {
            String masked = Utils.maskPii(Utils.decodeBase64String(id));
            LOGGER.info("id " + id + "(" + masked + ") last found in " + idLastFoundDelta.get(id) + " is not found in partitions");
            inconsistencyFound = true;
        }

        HashSet<String> missingIds2 = new HashSet<>(idLastFoundPartition.keySet());
        missingIds2.removeAll(idLastFoundDelta.keySet());
        for (String id : missingIds2) {
            LOGGER.info("id " + id + " last found in " + idLastFoundPartition.get(id) + " is not found in deltas");
            inconsistencyFound = true;
        }

        if (inconsistencyFound) {
            LOGGER.info("Consistency check pack(deltas) == pack (partitions) failed");
            return null;
        } else {
            LOGGER.info("Consistency check pack(deltas) == pack (partitions) passed");
            String mdFile = writeMetadataFile(packedDeltaOutFile, deltas, partitions);
            LOGGER.info("Packed file metadata saved as " + mdFile + ", ready to be pushed");
            return packedDeltaOutFile;
        }
    }

    private void runDump(String dataDir, String id) throws IOException {
        JsonObject mergedConfig = new JsonObject().mergeIn(config)
            .put(Const.Config.OptOutDataDirProp, dataDir);
        this.config = mergedConfig;
        File partitionDir = new File(OptOutUtils.getPartitionConsumerDir(config));
        File deltaDir = new File(OptOutUtils.getDeltaConsumerDir(config));

        List<File> files = new ArrayList<>();
        files.addAll(Arrays.asList(partitionDir.listFiles()));
        files.addAll(Arrays.asList(deltaDir.listFiles()));
        LOGGER.info("There are " + files.size() + " files found under " + dataDir);

        for (File f : files) {
            byte[] data = Files.readAllBytes(f.toPath());
            OptOutCollection coll = new OptOutCollection(data);
            for (int i = 0; i < coll.size(); ++i) {
                OptOutEntry entry = coll.get(i);
                if (entry.idHashToB64().equals(id)) {
                    dumpEntryAsJson(entry);
                    LOGGER.info("\n  ^^^ Found in " + f.getName());
                }
            }
        }
    }

    private void runPush(String file) throws Exception {
        if (!Files.exists(Paths.get(file))) {
            throw new Exception("File not found: " + file);
        }

        String metadataFile = file + ".metadata.txt";
        if (!Files.exists(Paths.get(metadataFile))) {
            throw new Exception("Metadata file not found: " + file);
        }

        final ICloudStorage cloudStorage;
        if (!this.isYes) {
            LOGGER.info("Dry-run mode (specify -yes to disable)");
            cloudStorage = new DryRunStorageMock(isVerbose);
        } else {
            String bucket = config.getString(Const.Config.OptOutS3BucketProp);
            cloudStorage = wrapCloudStorageForOptOut(CloudUtils.createStorage(bucket, config));
        }

        OptOutCloudSync cloudSync = new OptOutCloudSync(config, false);

        String cloudPathForPartition = cloudSync.toCloudPath(file);
        LOGGER.info("Uploading packed file as partition: " + file + " -> " + cloudPathForPartition);
        cloudStorage.upload(file, cloudPathForPartition);

        String cloudPathForDelta = cloudSync.toCloudPath(file.replace("partition", "delta"));
        LOGGER.info("Uploading packed file as delta: " + file + " -> " + cloudPathForDelta);
        cloudStorage.upload(file, cloudPathForDelta);

        Scanner inFile = new Scanner(new File(metadataFile));
        List<String> filesToDelete = new ArrayList<>();

        while(inFile.hasNext()) {
            filesToDelete.add(inFile.nextLine());
        }

        int counter = 0;
        if (filesToDelete.size() < 1000) {
            if (!this.isVerbose && filesToDelete.size() > 0) {
                System.out.print("Deleting from cloud storage: ");
            }

            for (String fileToDelete : filesToDelete) {
                String r = cloudSync.toCloudPath(fileToDelete);
                cloudStorage.delete(r);

                if (!this.isVerbose) {
                    System.out.print(".");
                    if (++counter > 80) {
                        counter = 0;
                        System.out.println();
                    }
                }
            }
        } else {
            Collection<String> cloudPaths = filesToDelete.stream()
                .map(p -> cloudSync.toCloudPath(p))
                .collect(Collectors.toList());
            cloudStorage.delete(cloudPaths);
            LOGGER.info("batch deleted " + cloudPaths.size() + " cloud paths");
        }

        LOGGER.info("\nSuccess!");
        LOGGER.info("1 packed file uploaded, " + filesToDelete.size() + " files deleted.");
    }

    private void runVerify(String file, String url) throws IOException {
        File f = new File(file);
        byte[] data = Files.readAllBytes(f.toPath());
        OptOutCollection coll = new OptOutCollection(data);

        int foundErrors = 0;
        for (int i = 0; i < coll.size(); ++i) {
            OptOutEntry entry = coll.get(i);
            if (entry.isSpecialHash()) continue;

            String queryString = String.format("?email_hash=%s",
                URLEncoder.encode(entry.idHashToB64(), "ASCII"));
            URL request = new URL(url + queryString);
            String body = Utils.readToEnd(request.openStream());
            if (entry.timestamp != (long)Long.valueOf(body)) {
                System.err.format("Verification failed: %s expect optout time %d, api returned %s\n",
                    entry.idHashToB64(), entry.timestamp, body);
                ++foundErrors;
            } else {
                System.out.format("Verified: %s optout time %d\n", entry.idHashToB64(), entry.timestamp);
            }
        }

        LOGGER.info("Verification result: " + foundErrors + " errors");
    }

    // generate synthetic optout entries
    private void runGenerate(String outDir, String salt, int count) throws NoSuchAlgorithmException, IOException {
        Utils.ensureDirectoryExists(outDir);
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        final int numEntriesPerPartition = 1000000;
        int numPartitionFiles = (count + numEntriesPerPartition - 1) / numEntriesPerPartition;
        int entryId = 0;
        for (int i = 0; i < numPartitionFiles; ++i) {
            OptOutHeap heap = new OptOutHeap(numEntriesPerPartition);

            for (int j = 0; j < numEntriesPerPartition; ++j) {
                String email = String.format("%08d@uidapi.com", entryId++);
                byte[] emailHashBytes = digest.digest(email.getBytes(StandardCharsets.UTF_8));
                String firstLevelId = Utils.toBase64String(emailHashBytes) + salt;
                byte[] firstLevelHashBytes = digest.digest(firstLevelId.getBytes(StandardCharsets.UTF_8));

                OptOutEntry entry = new OptOutEntry(firstLevelHashBytes, firstLevelHashBytes,
                    Instant.now().getEpochSecond());
                heap.add(entry);
            }

            OptOutPartition partition = heap.toPartition(true);
            String fn = newSyntheticPartitionFileName(outDir, Instant.now());
            LOGGER.info("Generated synthetic partition file (" + numEntriesPerPartition  + " entries): " + fn);
            Files.write(Paths.get(fn), partition.getStore());
        }
    }

    private String packDir(String inDir, String outDir, Map<String , String> idLastFound, Set<String> packedFiles,
                           Instant deadline) throws IOException {
        File dir = new File(inDir);
        File[] files = dir.listFiles();
        LOGGER.info("There are " + files.length + " files found under " + inDir);
        System.out.print("Packing");
        OptOutHeap heap = new OptOutHeap(files.length * 4);
        int counter = 0;
        long minTimeStamp = -1;
        for (File f : files) {
            // skip files that are after the specified deadline
            Instant ts = OptOutUtils.getFileTimestamp(f.getName());
            if (ts.isAfter(deadline)) continue;

            byte[] data = Files.readAllBytes(f.toPath());
            int entriesAdded = 0;
            OptOutCollection coll = new OptOutCollection(data);
            for (int i = 0; i < coll.size(); ++i) {
                OptOutEntry entry = coll.get(i);
                if (entry.isSpecialHash()) {
                    if (Arrays.equals(entry.identityHash, OptOutUtils.nullHashBytes)) {
                        // for null hash (all zero bits), only add if the timestamp is smaller
                        if (entry.timestamp < minTimeStamp || minTimeStamp == -1) {
                            minTimeStamp = entry.timestamp;
                            heap.add(entry);
                        }
                    } else {
                        // for ones hash (all one bits), add the entry, largest timestamp will be kept after merging
                        heap.add(entry);
                    }
                    continue;
                }

                heap.add(entry);
                idLastFound.put(entry.idHashToB64(), f.getName());
                ++entriesAdded;
            }

            if (entriesAdded == 0) {
                System.out.print(".");
                if (++counter > 80) {
                    counter = 0;
                    System.out.println();
                }
            } else {
                counter = 0;
                System.out.println("<" + entriesAdded + ">");
            }

            packedFiles.add(f.getName());
        }
        System.out.println();

        OptOutPartition partition = heap.toPartition(true);
        LOGGER.info("Packed file has " + partition.size() + " entries");
        if (partition.size() < 256 || this.isVerbose) {
            int total = partition.size();
            for (int i = 0; i < total; ++i) {
                dumpEntryAsJson(partition.get(i));
            }
        }

        // last entry (all ones) should have the largest timestamp
        long timestamp = partition.get(partition.size() - 1).timestamp;
        String newPartFile = newPartitionFileName(outDir, Instant.ofEpochSecond(timestamp));
        Files.write(Paths.get(newPartFile), partition.getStore());
        return newPartFile;
    }

    private String writeMetadataFile(String file, Collection<String> deltas, Collection<String> partitions) throws IOException {
        Set<String> packedFiles = new HashSet<>(deltas);
        packedFiles.addAll(partitions);
        String metaDataFile = file + ".metadata.txt";
        String metadataContent = String.join("\n", packedFiles);
        Files.write(Paths.get(metaDataFile), metadataContent.getBytes());
        return metaDataFile;
    }

    private void dumpEntryAsJson(OptOutEntry entry) {
        String msg = String.format("{ \"identity_hash\": \"%s\", \"advertising_id\": \"%s\", \"timestamp\": %d }",
            Utils.maskPii(entry.identityHash),
            Utils.maskPii(entry.advertisingId),
            entry.timestamp);
        LOGGER.info(msg);
    }

    private void runGc() throws CloudStorageException {
        // always assuming synthetic logs are enabled when doing the purge
        JsonObject mergedConfig = new JsonObject().mergeIn(config)
            .put(Const.Config.OptOutSyntheticLogsEnabledProp, true)
            .put(Const.Config.OptOutSyntheticLogsCountProp, 0);
        this.config = mergedConfig;

        final boolean deleteExpiredEnabled = this.config.getBoolean(Const.Config.OptOutDeleteExpiredProp);
        LOGGER.info("Delete Expired Log Enabled: " + deleteExpiredEnabled);
        if (!deleteExpiredEnabled) return;

        String bucket = config.getString(Const.Config.OptOutS3BucketProp);
        ICloudStorage cloudStorage = wrapCloudStorageForOptOut(CloudUtils.createStorage(bucket, config));
        FileUtils utils = new FileUtils(config);

        List<String> expiredLogs = new ArrayList<>();
        String cloudOptOutFolder = config.getString(Const.Config.OptOutS3FolderProp);
        Instant now = Instant.now();
        for (String f : cloudStorage.list(cloudOptOutFolder)) {
            if (!OptOutUtils.isDeltaFile(f) && !OptOutUtils.isPartitionFile(f)) {
                LOGGER.info("Ignoring non-optout-log files: " + f);
                continue;
            }
            if (utils.isDeltaOrPartitionExpired(now, f)) {
                LOGGER.debug("Scheduled to delete expired log: " + f);
                expiredLogs.add(f);
            }
        }

        if (expiredLogs.size() == 0) {
            LOGGER.info("Found 0 expired logs");
        }

        final ICloudStorage deletingCloudStorage;
        if (!this.isYes) {
            LOGGER.info("Dry-run mode (specify -yes to disable)");
            deletingCloudStorage = new DryRunStorageMock(isVerbose);
        } else {
            deletingCloudStorage = cloudStorage;
        }

        LOGGER.info("Deleting " + expiredLogs.size() + " expired logs");
        deletingCloudStorage.delete(expiredLogs);
    }

    private void runPurge(String dataDir) throws CloudStorageException, IOException {
        dataDir = Paths.get(dataDir).toAbsolutePath().toString();
        Utils.ensureDirectoryExists(dataDir);

        JsonObject mergedConfig = new JsonObject().mergeIn(config)
            .put(Const.Config.OptOutDataDirProp, dataDir);
        this.config = mergedConfig;

        List<String> emptyDeltaLogs = new ArrayList<>();
        OptOutCloudSync cloudSync = new OptOutCloudSync(mergedConfig, true);

        String deltaDir = OptOutUtils.getDeltaConsumerDir(config);
        File dir = new File(deltaDir);
        File[] deltas = dir.listFiles();
        for (File f : deltas) {
            if (!OptOutUtils.isDeltaFile(f.getName())) continue;
            Path p = f.toPath();
            Instant ts = OptOutUtils.getFileTimestamp(p);
            Instant twoHoursAgo = Instant.now().minus(2, ChronoUnit.HOURS);
            if (ts.isAfter(twoHoursAgo)) {
                LOGGER.trace("Skipping delta generated within 2 hours: " + f.getName());
                continue;
            }

            byte[] data = Files.readAllBytes(p);
            boolean isEmpty = true;
            OptOutCollection coll = new OptOutCollection(data);
            for (int i = 0; i < coll.size(); ++i) {
                OptOutEntry entry = coll.get(i);
                if (!entry.isSpecialHash()) {
                    isEmpty = false;
                    break;
                }
            }

            if (isEmpty) {
                String local = f.getName();
                String remote = cloudSync.toCloudPath(local);
                emptyDeltaLogs.add(remote);
                LOGGER.trace("Scheduled to delete empty delta (size = " + data.length + "): " + remote);
            }
        }

        final ICloudStorage deletingCloudStorage;
        if (!this.isYes) {
            LOGGER.info("Dry-run mode (specify -yes to disable)");
            deletingCloudStorage = new DryRunStorageMock(isVerbose);
        } else {
            String bucket = config.getString(Const.Config.OptOutS3BucketProp);
            deletingCloudStorage = wrapCloudStorageForOptOut(CloudUtils.createStorage(bucket, config));
        }

        LOGGER.info("Deleting " + emptyDeltaLogs.size() + " empty delta");
        deletingCloudStorage.delete(emptyDeltaLogs);
    }

    private void runCronJob(boolean doPack) throws Exception {
        // An OptOut CronJob performs the following tasks in sequentially
        // Step 1. gc    (deleting expired logs, including deltas and partitions)
        // Step 2. sync  (downloading logs from s3)
        // Step 3. purge (deleting empty deltas)
        // Step 4. sync  (sync again after delete)
        // -- the following 3 steps are optional unless -pack is specified
        // Step 5. pack  (compact deltas)
        // Step 6. push  (push compacted deltas)
        // Step 7. sync  (sync again after compact)

        // Step 1.
        runGc();

        // Step 2.
        // CronJob uses the default optout_data_dir to sync
        String dataDir = config.getString(Const.Config.OptOutDataDirProp);
        runSync(dataDir);

        // Step 3.
        runPurge(dataDir);

        // Step 4.
        runSync(dataDir);

        if (!doPack) return;

        // Step 5.
        String outDir = Paths.get(dataDir, "workdir").toString();
        String packedFile = runPack(dataDir, outDir, true);
        if (packedFile == null) System.exit(1);

        // Step 6.
        runPush(packedFile);

        // Step 7.
        runSync(dataDir);
    }

    private String newPartitionFileName(String outDir, Instant ts) {
        return Paths.get(outDir, OptOutUtils.newPartitionFileName(ts)).toString();
    }

    private String newSyntheticPartitionFileName(String outDir, Instant now) {
        String ts = OptOutUtils.timestampEscaped(now.truncatedTo(ChronoUnit.DAYS));
        return Paths.get(outDir, OptOutUtils.newPartitionFileName(ts, 999)).toString();
    }
}
