/**
 * SPDX-FileCopyrightText: 2020 Crawler-commons SPDX-License-Identifier: Apache-2.0 Licensed to
 * Crawler-Commons under one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership. DigitalPebble licenses
 * this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package crawlercommons.urlfrontier.service.rocksdb;

import com.google.protobuf.InvalidProtocolBufferException;
import crawlercommons.urlfrontier.CrawlID;
import crawlercommons.urlfrontier.Urlfrontier.KnownURLItem;
import crawlercommons.urlfrontier.Urlfrontier.Stats;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import crawlercommons.urlfrontier.Urlfrontier.URLItem;
import crawlercommons.urlfrontier.service.AbstractFrontierService;
import crawlercommons.urlfrontier.service.QueueInterface;
import crawlercommons.urlfrontier.service.QueueWithinCrawl;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.slf4j.LoggerFactory;

public class RocksDBService extends AbstractFrontierService implements Closeable {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RocksDBService.class);

    private static final DecimalFormat DF = new DecimalFormat("0000000000");

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB rocksDB;

    // a list which will hold the handles for the column families once the db is
    // opened
    private final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

    private Statistics statistics;

    // no explicit config
    public RocksDBService() {
        this(new HashMap<String, String>());
    }

    private final ConcurrentHashMap<QueueWithinCrawl, QueueWithinCrawl> queuesBeingDeleted =
            new ConcurrentHashMap<>();

    public RocksDBService(final Map<String, String> configuration) {

        // where to store it?
        String path = configuration.getOrDefault("rocksdb.path", "./rocksdb");

        LOG.info("RocksDB data stored in {} ", path);

        if (configuration.containsKey("rocksdb.purge")) {
            try {
                Files.walk(Paths.get(path))
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                LOG.error("Couldn't delete path {}", path);
            }
        }

        if (configuration.containsKey("rocksdb.stats")) {
            statistics = new Statistics();
            statistics.setStatsLevel(StatsLevel.ALL);
        }

        boolean bloomFilters = configuration.containsKey("rocksdb.bloom.filters");

        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {

            String sMaxBytesForLevelBase = configuration.get("rocksdb.max_bytes_for_level_base");
            if (sMaxBytesForLevelBase != null) {
                cfOpts.setMaxBytesForLevelBase(Long.parseLong(sMaxBytesForLevelBase));
            }

            cfOpts.optimizeUniversalStyleCompaction();

            // list of column family descriptors, first entry must always be default column
            // family
            final List<ColumnFamilyDescriptor> cfDescriptors =
                    Arrays.asList(
                            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                            new ColumnFamilyDescriptor("queues".getBytes(), cfOpts));

            long start = System.currentTimeMillis();

            if (bloomFilters) {
                LOG.info("Configuring Bloom filters");
                cfOpts.setTableFormatConfig(
                        new BlockBasedTableConfig().setFilterPolicy(new BloomFilter(10, false)));
            }

            try (final DBOptions options = new DBOptions()) {
                options.setCreateIfMissing(true).setCreateMissingColumnFamilies(true);

                String smaxBackgroundJobs = configuration.get("rocksdb.max_background_jobs");
                if (smaxBackgroundJobs != null) {
                    options.setMaxBackgroundJobs(Integer.parseInt(smaxBackgroundJobs));
                }

                // Options.max_subcompactions: 1
                String smax_subcompactions = configuration.get("rocksdb.max_subcompactions");
                if (smax_subcompactions != null) {
                    options.setMaxSubcompactions(Integer.parseInt(smax_subcompactions));
                }

                if (statistics != null) {
                    LOG.info("Allowing stats from RocksDB to be displayed when GetStats is called");
                    options.setStatistics(statistics);
                }

                rocksDB = RocksDB.open(options, path, cfDescriptors, columnFamilyHandleList);
            } catch (RocksDBException e) {
                LOG.error("RocksDB exception ", e);
                throw new RuntimeException(e);
            }

            long end = System.currentTimeMillis();

            LOG.info("RocksDB loaded in {} msec", end - start);

            LOG.info("Scanning tables to rebuild queues... (can take a long time)");

            recoveryQscan();

            long end2 = System.currentTimeMillis();

            LOG.info("{} queues discovered in {} msec", queues.size(), (end2 - end));
        }
    }

    /** Resurrects the queues from the tables and does sanity checks * */
    private void recoveryQscan() {

        LOG.info("Recovering queues from existing RocksDB");

        try (final RocksIterator rocksIterator =
                rocksDB.newIterator(columnFamilyHandleList.get(1))) {
            for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
                final String currentKey = new String(rocksIterator.key(), StandardCharsets.UTF_8);
                final QueueWithinCrawl qk = parseAndDeNormalise(currentKey);
                queues.putIfAbsent(qk, new QueueMetadata());
                ((QueueMetadata) queues.get(qk)).incrementActive();
            }
        }

        QueueWithinCrawl previousQueueID = null;
        long numScheduled = 0;

        // now get the counts of URLs already finished
        try (final RocksIterator rocksIterator =
                rocksDB.newIterator(columnFamilyHandleList.get(0))) {
            for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
                final String currentKey = new String(rocksIterator.key(), StandardCharsets.UTF_8);
                final QueueWithinCrawl Qkey = parseAndDeNormalise(currentKey);

                // changed ID? check that the previous one had the correct values
                if (previousQueueID == null) {
                    previousQueueID = Qkey;
                } else if (!previousQueueID.equals(Qkey)) {
                    if (queues.get(previousQueueID).countActive() != numScheduled)
                        throw new RuntimeException(
                                "Incorrect number of active URLs for queue " + previousQueueID);
                    previousQueueID = Qkey;
                    numScheduled = 0;
                }

                // queue might not exist if it had nothing scheduled for it
                // i.e. all done
                queues.putIfAbsent(Qkey, new QueueMetadata());

                // check the value - if it is an empty byte array it means that the URL has been
                // processed and is not scheduled
                // otherwise it is scheduled
                boolean done = rocksIterator.value().length == 0;
                if (done) {
                    ((QueueMetadata) queues.get(Qkey)).incrementActive();
                } else {
                    // double check the number of scheduled later on
                    numScheduled++;
                }
            }
        }
        // check the last key
        if (previousQueueID != null && queues.get(previousQueueID).countActive() != numScheduled) {
            throw new RuntimeException(
                    "Incorrect number of active URLs for queue " + previousQueueID);
        }
    }

    @Override
    protected int sendURLsForQueue(
            QueueInterface queue,
            QueueWithinCrawl queueID,
            int maxURLsPerQueue,
            int secsUntilRequestable,
            long now,
            StreamObserver<URLInfo> responseObserver) {

        int alreadySent = 0;
        final byte[] prefixKey = (normalise(queueID) + "_").getBytes(StandardCharsets.UTF_8);
        // scan the scheduling table
        try (final RocksIterator rocksIterator =
                rocksDB.newIterator(columnFamilyHandleList.get(1))) {
            for (rocksIterator.seek(prefixKey);
                    rocksIterator.isValid() && alreadySent < maxURLsPerQueue;
                    rocksIterator.next()) {

                final String currentKey = new String(rocksIterator.key(), StandardCharsets.UTF_8);

                // don't want to split the whole string _ as the URL part is left as is
                final int pos = currentKey.indexOf('_');
                final int pos2 = currentKey.indexOf('_', pos + 1);
                final int pos3 = currentKey.indexOf('_', pos2 + 1);

                final String crawlPart = currentKey.substring(0, pos);
                final String queuePart = currentKey.substring(pos + 1, pos2);
                final String urlPart = currentKey.substring(pos3 + 1);

                // not for this queue anymore?
                if (!queueID.equals(crawlPart, queuePart)) {
                    return alreadySent;
                }

                // too early for it?
                long scheduled = Long.parseLong(currentKey.substring(pos2 + 1, pos3));
                if (scheduled > now) {
                    // they are sorted by date no need to go further
                    return alreadySent;
                }

                // check that the URL is not already being processed
                if (((QueueMetadata) queue).isHeld(urlPart, now)) {
                    continue;
                }

                // this one is good to go
                try {
                    responseObserver.onNext(URLInfo.parseFrom(rocksIterator.value()));

                    // mark it as not processable for N secs
                    ((QueueMetadata) queue).holdUntil(urlPart, now + secsUntilRequestable);

                    alreadySent++;
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Caught unlikely error ", e);
                }
            }
        }

        return alreadySent;
    }

    @Override
    public StreamObserver<URLItem> putURLs(
            StreamObserver<crawlercommons.urlfrontier.Urlfrontier.String> responseObserver) {

        return new StreamObserver<URLItem>() {

            @Override
            public void onNext(URLItem value) {

                long nextFetchDate;
                boolean discovered = true;
                URLInfo info;

                if (value.hasDiscovered()) {
                    info = value.getDiscovered().getInfo();
                    nextFetchDate = Instant.now().getEpochSecond();
                } else {
                    KnownURLItem known = value.getKnown();
                    info = known.getInfo();
                    nextFetchDate = known.getRefetchableFromDate();
                    discovered = Boolean.FALSE;
                }

                String Qkey = info.getKey();
                String url = info.getUrl();
                String crawlID = CrawlID.normaliseCrawlID(info.getCrawlID());

                // has a queue key been defined? if not use the hostname
                if (Qkey.equals("")) {
                    LOG.debug("key missing for {}", url);
                    Qkey = provideMissingKey(url);
                    if (Qkey == null) {
                        LOG.error("Malformed URL {}", url);
                        responseObserver.onNext(
                                crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                        .setValue(url)
                                        .build());
                        return;
                    }
                    // make a new info object ready to return
                    info = URLInfo.newBuilder(info).setKey(Qkey).setCrawlID(crawlID).build();
                }

                // check that the key is not too long
                if (Qkey.length() > 255) {
                    LOG.error("Key too long: {}", Qkey);
                    responseObserver.onNext(
                            crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                    .setValue(url)
                                    .build());
                    return;
                }

                QueueWithinCrawl qk = QueueWithinCrawl.get(Qkey, crawlID);

                // ignore this url if the queue is being deleted
                if (queuesBeingDeleted.containsKey(qk)) {
                    LOG.info("Not adding {} as its queue {} is being deleted", url, Qkey);
                    responseObserver.onNext(
                            crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                    .setValue(url)
                                    .build());
                    return;
                }

                byte[] schedulingKey = null;

                final byte[] existenceKey =
                        (normalise(qk) + "_" + url).getBytes(StandardCharsets.UTF_8);

                // is this URL already known?
                try {
                    schedulingKey = rocksDB.get(existenceKey);
                } catch (RocksDBException e) {
                    LOG.error("RocksDB exception", e);
                    // TODO notify the client
                    return;
                }

                // already known? ignore if discovered
                if (schedulingKey != null && discovered) {
                    responseObserver.onNext(
                            crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                    .setValue(url)
                                    .build());
                    return;
                }

                // get the priority queue or create one
                QueueMetadata queueMD = new QueueMetadata();
                QueueMetadata previous = (QueueMetadata) queues.putIfAbsent(qk, queueMD);
                if (previous != null) {
                    queueMD = previous;
                }

                try {
                    // known - remove from queues
                    // its key in the queues was stored in the default cf
                    if (schedulingKey != null) {
                        rocksDB.delete(columnFamilyHandleList.get(1), schedulingKey);
                        // remove from queue metadata
                        queueMD.removeFromProcessed(url);
                        queueMD.decrementActive();
                    }

                    // add the new item
                    // unless it is an update and it's nextFetchDate is 0 == NEVER
                    if (!discovered && nextFetchDate == 0) {
                        // does not need scheduling
                        // remove any scheduling key from its value
                        schedulingKey = new byte[] {};
                        queueMD.incrementCompleted();
                    } else {
                        // it is either brand new or already known
                        // create a scheduling key for it
                        schedulingKey =
                                (normalise(qk) + "_" + DF.format(nextFetchDate) + "_" + url)
                                        .getBytes(StandardCharsets.UTF_8);
                        // add to the scheduling
                        rocksDB.put(
                                columnFamilyHandleList.get(1), schedulingKey, info.toByteArray());
                        queueMD.incrementActive();
                    }
                    // update the link to its queue
                    // TODO put in a batch? rocksDB.write(new WriteOptions(), writeBatch);
                    rocksDB.put(columnFamilyHandleList.get(0), existenceKey, schedulingKey);

                } catch (RocksDBException e) {
                    LOG.error("RocksDB exception", e);
                }

                responseObserver.onNext(
                        crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                .setValue(url)
                                .build());
            }

            @Override
            public void onError(Throwable t) {
                LOG.error("Throwable caught", t);
            }

            @Override
            public void onCompleted() {
                // will this ever get called if the client is constantly streaming?
                responseObserver.onCompleted();
            }
        };
    }

    /**
     *
     *
     * <pre>
     * * Delete  the queue based on the key in parameter *
     * </pre>
     */
    @Override
    public void deleteQueue(
            crawlercommons.urlfrontier.Urlfrontier.QueueWithinCrawlParams request,
            StreamObserver<crawlercommons.urlfrontier.Urlfrontier.Integer> responseObserver) {

        final QueueWithinCrawl qc = QueueWithinCrawl.get(request.getKey(), request.getCrawlID());

        int sizeQueue = 0;

        // is this queue already being deleted?
        // no need to do it again
        if (queuesBeingDeleted.contains(qc)) {
            responseObserver.onNext(
                    crawlercommons.urlfrontier.Urlfrontier.Integer.newBuilder()
                            .setValue(sizeQueue)
                            .build());
            responseObserver.onCompleted();
            return;
        }

        queuesBeingDeleted.put(qc, qc);

        // find the next key by alphabetical order
        QueueWithinCrawl[] array = queues.getKeys();
        Arrays.sort(array);
        byte[] startKey = null;
        byte[] endKey = null;
        for (QueueWithinCrawl prefixed_queue : array) {
            if (startKey != null) {
                endKey = (normalise(prefixed_queue) + "_").getBytes(StandardCharsets.UTF_8);
                break;
            } else if (prefixed_queue.equals(qc)) {
                startKey = (normalise(qc) + "_").getBytes(StandardCharsets.UTF_8);
            }
        }

        try {
            deleteRanges(startKey, endKey);
        } catch (RocksDBException e) {
            LOG.error(
                    "Exception caught when deleting ranges - {} - {}",
                    new String(startKey),
                    new String(endKey));
        }

        QueueInterface q = queues.remove(qc);
        sizeQueue += q.countActive();
        sizeQueue += q.getCountCompleted();

        queuesBeingDeleted.remove(qc);

        responseObserver.onNext(
                crawlercommons.urlfrontier.Urlfrontier.Integer.newBuilder()
                        .setValue(sizeQueue)
                        .build());
        responseObserver.onCompleted();
    }

    /** underscores being used as separator for the keys */
    public static final String normalise(QueueWithinCrawl qwc) {
        return qwc.getCrawlid().replaceAll("_", "%5F")
                + "_"
                + qwc.getQueue().replaceAll("_", "%5F");
    }

    public static final QueueWithinCrawl parseAndDeNormalise(String currentKey) {
        final int pos = currentKey.indexOf('_');
        final String crawlID = currentKey.substring(0, pos).replaceAll("%5F", "_");
        final int pos2 = currentKey.indexOf('_', pos + 1);
        final String queueID = currentKey.substring(pos + 1, pos2).replaceAll("%5F", "_");
        return QueueWithinCrawl.get(queueID, crawlID);
    }

    @Override
    public void getStats(
            crawlercommons.urlfrontier.Urlfrontier.QueueWithinCrawlParams request,
            StreamObserver<Stats> responseObserver) {
        if (statistics != null) {
            LOG.info("RockdSB stats: {}", statistics);
        }
        super.getStats(request, responseObserver);
    }

    @Override
    public void close() throws IOException {

        for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
            columnFamilyHandle.close();
        }

        if (statistics != null) {
            statistics.close();
        }

        if (rocksDB != null) {
            try {
                rocksDB.close();
            } catch (Exception e) {
                LOG.error("Closing ", e);
            }
        }
    }

    @Override
    public void deleteCrawl(
            crawlercommons.urlfrontier.Urlfrontier.String crawlID,
            io.grpc.stub.StreamObserver<crawlercommons.urlfrontier.Urlfrontier.Integer>
                    responseObserver) {

        long total = 0;

        final String normalisedCrawlID = CrawlID.normaliseCrawlID(crawlID.getValue());

        final Set<QueueWithinCrawl> toDelete = new HashSet<>();

        synchronized (queues) {

            // find the crawlIDs
            QueueWithinCrawl[] array = queues.getKeys();
            Arrays.sort(array);

            byte[] startKey = null;
            byte[] endKey = null;
            for (QueueWithinCrawl prefixed_queue : array) {
                boolean samePrefix = prefixed_queue.getCrawlid().equals(normalisedCrawlID);
                if (samePrefix) {
                    if (startKey == null) {
                        startKey =
                                (prefixed_queue.getCrawlid().replaceAll("_", "%5F") + "_")
                                        .getBytes(StandardCharsets.UTF_8);
                    }
                    toDelete.add(prefixed_queue);
                } else if (startKey != null) {
                    endKey =
                            (prefixed_queue.getCrawlid().replaceAll("_", "%5F") + "_")
                                    .getBytes(StandardCharsets.UTF_8);
                    break;
                }
            }

            try {
                deleteRanges(startKey, endKey);
            } catch (RocksDBException e) {
                LOG.error(
                        "Exception caught when deleting ranges - {} - {}",
                        new String(startKey),
                        new String(endKey));
            }

            for (QueueWithinCrawl quid : toDelete) {
                if (queuesBeingDeleted.contains(quid)) {
                    continue;
                } else {
                    queuesBeingDeleted.put(quid, quid);
                }

                QueueInterface q = queues.remove(quid);
                total += q.countActive();
                total += q.getCountCompleted();

                queuesBeingDeleted.remove(quid);
            }
        }
        responseObserver.onNext(
                crawlercommons.urlfrontier.Urlfrontier.Integer.newBuilder()
                        .setValue(total)
                        .build());
        responseObserver.onCompleted();
    }

    private void deleteRanges(final byte[] prefix, byte[] endKey) throws RocksDBException {

        // if endKey is null it means that there is no other crawlID after this one
        boolean includeEndKey = false;

        if (endKey == null) {
            try (RocksIterator iter = rocksDB.newIterator(columnFamilyHandleList.get(0))) {
                iter.seekToLast();
                if (iter.isValid()) {
                    // this is the last known URL
                    endKey = iter.key();
                    includeEndKey = true;
                }
            }
        }

        // no end key found?
        if (endKey == null) {
            throw new RuntimeException("No endkey found");
        }

        // delete the ranges in the queues table as well as the URLs already
        // processed
        rocksDB.deleteRange(columnFamilyHandleList.get(1), prefix, endKey);
        rocksDB.deleteRange(columnFamilyHandleList.get(0), prefix, endKey);

        if (includeEndKey) {
            rocksDB.deleteRange(columnFamilyHandleList.get(1), endKey, endKey);
            rocksDB.delete(columnFamilyHandleList.get(0), endKey);
        }
    }
}
