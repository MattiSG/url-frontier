/**
 * SPDX-FileCopyrightText: 2022 Crawler-commons SPDX-License-Identifier: Apache-2.0 Licensed to
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
package crawlercommons.urlfrontier.service.ignite;

import com.google.protobuf.InvalidProtocolBufferException;
import crawlercommons.urlfrontier.CrawlID;
import crawlercommons.urlfrontier.Urlfrontier.KnownURLItem;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import crawlercommons.urlfrontier.Urlfrontier.URLItem;
import crawlercommons.urlfrontier.service.QueueInterface;
import crawlercommons.urlfrontier.service.QueueWithinCrawl;
import crawlercommons.urlfrontier.service.cluster.DistributedFrontierService;
import crawlercommons.urlfrontier.service.cluster.HeartbeatListener;
import crawlercommons.urlfrontier.service.rocksdb.QueueMetadata;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache.Entry;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

public class IgniteService extends DistributedFrontierService
        implements Closeable, HeartbeatListener {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(IgniteService.class);

    public static final String frontiersCacheName = "frontiers";

    private static final String URLCacheNamePrefix = "urls_";

    private final Ignite ignite;

    private final ConcurrentHashMap<QueueWithinCrawl, QueueWithinCrawl> queuesBeingDeleted =
            new ConcurrentHashMap<>();

    private final IgniteCache<String, String> globalQueueCache;

    private boolean closing = false;

    private final IgniteHeartbeat ihb;

    private final IndexWriter iwriter;

    private IndexSearcher isearcher;

    // no explicit config
    public IgniteService() {
        this(new HashMap<String, String>());
    }

    public IgniteService(final Map<String, String> configuration) {

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Classes of custom Java logic will be transferred over the wire from this app.
        cfg.setPeerClassLoadingEnabled(true);

        // "127.0.0.1:47500..47509"
        String igniteSeedAddress = configuration.get("ignite.seed.address");
        if (igniteSeedAddress != null) {
            clusterMode = true;
            // Setting up an IP Finder to ensure the client can locate the servers.
            TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
            ipFinder.setAddresses(Collections.singletonList(igniteSeedAddress));
            cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
        }

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        // specify where the data should be kept
        String path = configuration.get("ignite.path");
        if (path != null) {
            if (configuration.containsKey("ignite.purge")) {
                try {
                    Files.walk(Paths.get(path))
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                } catch (IOException e) {
                    LOG.error("Couldn't delete path {}", path);
                }
            }
            storageCfg.setStoragePath(path);
        }

        // set the work directory to the same location unless overridden
        String workdir = configuration.getOrDefault("ignite.workdir", path);
        if (workdir != null) {
            if (configuration.containsKey("ignite.purge")) {
                try {
                    Files.walk(Paths.get(workdir))
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                } catch (IOException e) {
                    LOG.error("Couldn't delete workdir {}", workdir);
                }
            }
            cfg.setWorkDirectory(workdir);
        }

        // set persistence
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        cfg.setDataStorageConfiguration(storageCfg);

        Path index_path = Paths.get(configuration.getOrDefault("ignite.index", "index"));
        // the index is always recreated from scratch
        try {
            Files.walk(index_path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            LOG.error("Couldn't delete workdir {}", workdir);
        }

        Analyzer analyzer = new StandardAnalyzer();

        try {
            Directory directory = FSDirectory.open(index_path);
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            iwriter = new IndexWriter(directory, config);
            DirectoryReader ireader = DirectoryReader.open(iwriter);
            isearcher = new IndexSearcher(ireader);
        } catch (IOException e) {
            LOG.error("Couldn't initialise Lucene writer {}", e);
            throw new RuntimeException(e);
        }

        long start = System.currentTimeMillis();

        // Starting the node
        ignite = Ignition.start(cfg);

        if (ignite.cluster().state().equals(ClusterState.INACTIVE)) {
            ignite.cluster().state(ClusterState.ACTIVE);
        }

        ClusterNode currentNode = ignite.cluster().localNode();
        @Nullable
        Collection<BaselineNode> baselineTopo = ignite.cluster().currentBaselineTopology();

        if (baselineTopo != null && !baselineTopo.contains(currentNode)) {
            baselineTopo.add(currentNode);
            // turn it off temporarily
            ignite.cluster().baselineAutoAdjustEnabled(false);
            ignite.cluster().setBaselineTopology(baselineTopo);
        }

        ignite.cluster().baselineAutoAdjustEnabled(true);
        ignite.cluster().baselineAutoAdjustTimeout(60000);

        int backups = Integer.parseInt(configuration.getOrDefault("ignite.backups", "3"));

        // template for cache configurations
        CacheConfiguration cacheCfg = new CacheConfiguration("urls_*");
        cacheCfg.setBackups(backups);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        // cacheCfg.setIndexedTypes(Key.class, Payload.class);
        ignite.addCacheConfiguration(cacheCfg);

        long end = System.currentTimeMillis();

        LOG.info("Ignite loaded in {} msec", end - start);

        IgniteEvents events = ignite.events();

        // Local listener that listens to local events.
        IgnitePredicate<CacheEvent> localListener =
                evt -> {
                    if (evt.key() instanceof Key == false) return true;

                    // TODO deal with updates?
                    final QueueWithinCrawl qk =
                            QueueWithinCrawl.parseAndDeNormalise(((Key) evt.key()).crawlQueueID);
                    try {
                        indexKeyValue(
                                qk,
                                ((Key) evt.key()).URL,
                                ((Payload) evt.newValue()).nextFetchDate);
                    } catch (Exception e) {
                        // TODO
                    }
                    return true; // Continue listening.
                };

        // Subscribe to the cache events that are triggered on the local node.
        events.localListen(localListener, EventType.EVT_CACHE_OBJECT_PUT);

        LOG.info("Scanning tables to rebuild queues... (can take a long time)");

        try {
            recoveryQscan(true);
        } catch (IOException e) {
            LOG.error("Exception while rebuilding the content", e);
            throw new RuntimeException(e);
        }

        long end2 = System.currentTimeMillis();

        LOG.info("{} queues discovered in {} msec", queues.size(), (end2 - end));

        // all the queues across the frontier instances
        CacheConfiguration cacheCfgQueues = new CacheConfiguration("queues");
        cacheCfgQueues.setBackups(backups);
        cacheCfgQueues.setCacheMode(CacheMode.PARTITIONED);
        globalQueueCache = ignite.getOrCreateCache(cacheCfgQueues);

        int heartbeatdelay =
                Integer.parseInt(configuration.getOrDefault("ignite.frontiers.heartbeat", "60"));
        int ttlFrontiers =
                Integer.parseInt(
                        configuration.getOrDefault(
                                "ignite.frontiers.ttl", Integer.toString(heartbeatdelay * 2)));

        // heartbeats of Frontiers
        CacheConfiguration cacheCfgFrontiers = new CacheConfiguration(frontiersCacheName);
        cacheCfgFrontiers.setBackups(backups);
        cacheCfgFrontiers.setCacheMode(CacheMode.REPLICATED);
        cacheCfgFrontiers.setExpiryPolicyFactory(
                ModifiedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, ttlFrontiers)));
        ignite.getOrCreateCache(cacheCfgFrontiers);

        // check the global queue list
        // every minute
        new QueueCheck(60).start();

        // start the heartbeat
        ihb = new IgniteHeartbeat(heartbeatdelay, ignite);
        ihb.setListener(this);
        ihb.start();
    }

    private IgniteCache<Key, Payload> createOrGetCacheForCrawlID(String crawlID) {
        return ignite.getOrCreateCache(URLCacheNamePrefix + crawlID);
    }

    @Override
    public void close() throws IOException {
        closing = true;
        LOG.info("Closing Ignite");
        if (ignite != null) ignite.close();
        if (ihb != null) ihb.close();
    }

    /**
     * Resurrects the queues from the tables *
     *
     * @throws IOException
     */
    private void recoveryQscan(boolean localMode) throws IOException {

        for (String cacheName : ignite.cacheNames()) {
            if (!cacheName.startsWith(URLCacheNamePrefix)) continue;

            IgniteCache<Key, Payload> cache = ignite.cache(cacheName);

            int urlsFound = 0;

            try (QueryCursor<Entry<Key, Payload>> cur =
                    cache.query(new ScanQuery<Key, Payload>().setLocal(localMode))) {
                for (Entry<Key, Payload> entry : cur) {
                    urlsFound++;
                    final QueueWithinCrawl qk =
                            QueueWithinCrawl.parseAndDeNormalise(entry.getKey().crawlQueueID);
                    QueueMetadata queueMD =
                            (QueueMetadata) queues.computeIfAbsent(qk, s -> new QueueMetadata());
                    // active if it has a scheduling value
                    boolean done = entry.getValue().nextFetchDate == 0;
                    if (done) {
                        queueMD.incrementCompleted();
                    } else {
                        queueMD.incrementActive();
                    }
                    cache.put(entry.getKey(), entry.getValue());

                    // index if not done
                    if (!done) {
                        indexKeyValue(qk, entry.getKey().URL, entry.getValue().nextFetchDate);
                    }
                }
            }
            LOG.info(
                    "Found {} URLs for crawl : {}",
                    urlsFound,
                    cacheName.substring(URLCacheNamePrefix.length()));
            iwriter.commit();
        }
    }

    private final void indexKeyValue(QueueWithinCrawl qk, String url, long nextFetchDate)
            throws IOException {
        Document doc = new Document();
        doc.add(new StringField("crawlid", qk.getCrawlid(), Store.NO));
        doc.add(new StringField("queue", qk.getQueue(), Store.NO));
        doc.add(new StringField("url", url, Store.YES));
        doc.add(new NumericDocValuesField("nextFetchDate", nextFetchDate));
        iwriter.addDocument(doc);
    }

    // periodically go through the list of queues
    // assigned to this node
    class QueueCheck extends Thread {

        private Instant lastQuery = Instant.EPOCH;

        private final int delaySec;

        QueueCheck(int delay) {
            delaySec = delay;
        }

        @Override
        public void run() {

            while (true) {
                if (closing) return;

                // implement delay between requests
                long msecTowait =
                        delaySec * 1000 - (Instant.now().toEpochMilli() - lastQuery.toEpochMilli());
                if (msecTowait > 0) {
                    try {
                        Thread.sleep(msecTowait);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                LOG.info("Checking queues");

                lastQuery = Instant.now();

                synchronized (queues) {
                    Set<QueueWithinCrawl> existingQueues =
                            new HashSet<QueueWithinCrawl>(queues.keySet());

                    int queuesFound = 0;
                    int queuesRemoved = 0;

                    // add any missing queues
                    try (QueryCursor<Entry<String, String>> cur =
                            globalQueueCache.query(
                                    new ScanQuery<String, String>().setLocal(true))) {
                        for (Entry<String, String> entry : cur) {
                            final QueueWithinCrawl qk =
                                    QueueWithinCrawl.parseAndDeNormalise(entry.getKey());
                            existingQueues.remove(qk);
                            queues.computeIfAbsent(qk, s -> new QueueMetadata());
                            queuesFound++;
                        }
                    }

                    // delete anything that would have been removed
                    for (QueueWithinCrawl remaining : existingQueues) {
                        queues.remove(remaining);
                        queuesRemoved++;
                    }

                    long time = Instant.now().toEpochMilli() - lastQuery.toEpochMilli();

                    LOG.info(
                            "Found {} queues, removed {}, total {} in {}",
                            queuesFound,
                            queuesRemoved,
                            queues.size(),
                            time);
                }
            }
        }
    }

    @Override
    public StreamObserver<URLItem> putURLs(
            StreamObserver<crawlercommons.urlfrontier.Urlfrontier.String> responseObserver) {

        putURLs_calls.inc();

        return new StreamObserver<URLItem>() {

            @Override
            public void onNext(URLItem value) {

                long nextFetchDate;
                boolean discovered = true;
                URLInfo info;

                putURLs_urls_count.inc();

                if (value.hasDiscovered()) {
                    putURLs_discovered_count.labels("true").inc();
                    info = value.getDiscovered().getInfo();
                    nextFetchDate = Instant.now().getEpochSecond();
                } else {
                    putURLs_discovered_count.labels("false").inc();
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

                Payload oldpayload = null;

                final Key existenceKey = new Key(qk.toString(), url);

                // if nextFetchDate == 0 give the payload a ridiculously large value
                // so that it is never refetched
                // ideally it should be removed from the index
                if (nextFetchDate == 0) {
                    nextFetchDate = Long.MAX_VALUE;
                }

                Payload newpayload = new Payload(nextFetchDate, info.toByteArray());

                // is this URL already known?
                try {
                    oldpayload =
                            (Payload)
                                    createOrGetCacheForCrawlID(crawlID)
                                            .getAndPut(existenceKey, newpayload);
                } catch (Exception e) {
                    LOG.error("Ignite exception", e);
                    return;
                }

                // already known? ignore if discovered
                if (oldpayload != null && discovered) {
                    putURLs_alreadyknown_count.inc();
                    responseObserver.onNext(
                            crawlercommons.urlfrontier.Urlfrontier.String.newBuilder()
                                    .setValue(url)
                                    .build());
                    return;
                }

                // get the priority queue - if it is a local one
                // or create a dummy one
                // but do not create it in the queues unless we are in a non distributed
                // environment
                QueueMetadata queueMD = null;

                if (clusterMode) {
                    queueMD = (QueueMetadata) queues.getOrDefault(qk, new QueueMetadata());
                } else {
                    queueMD = (QueueMetadata) queues.computeIfAbsent(qk, s -> new QueueMetadata());
                }

                // but make sure it exists globally anyway
                globalQueueCache.putIfAbsent(qk.toString(), qk.toString());

                // known - remove from queues
                // its key in the queues was stored in the default cf
                if (oldpayload != null) {
                    // remove from queue metadata
                    queueMD.removeFromProcessed(url);
                    queueMD.decrementActive();
                }

                // add the new item
                // unless it is an update and it's nextFetchDate is 0 == NEVER
                if (!discovered && nextFetchDate == 0) {
                    queueMD.incrementCompleted();
                    putURLs_completed_count.inc();
                } else {
                    // it is either brand new or already known
                    queueMD.incrementActive();
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

    @Override
    protected int sendURLsForQueue(
            QueueInterface queue,
            QueueWithinCrawl queueID,
            int maxURLsPerQueue,
            int secsUntilRequestable,
            long now,
            StreamObserver<URLInfo> responseObserver) {

        int alreadySent = 0;

        Builder query = new BooleanQuery.Builder();
        query.add(new TermQuery(new Term("crawlid", queueID.getCrawlid())), Occur.FILTER);
        query.add(new TermQuery(new Term("queue", queueID.getQueue())), Occur.FILTER);

        SortField sortField = new SortField("nextFetchDate", SortField.Type.INT, false);
        Sort sortByNextFetchDate = new Sort(sortField);

        IgniteCache<Key, Payload> _cache = createOrGetCacheForCrawlID(queueID.getCrawlid());

        try {
            ScoreDoc[] hits =
                    isearcher.search(query.build(), maxURLsPerQueue * 3, sortByNextFetchDate)
                            .scoreDocs;
            // Iterate through the results:
            for (int i = 0; i < hits.length && alreadySent >= maxURLsPerQueue; i++) {
                Document hitDoc = isearcher.doc(hits[i].doc);
                String url = hitDoc.get("url");

                // check that the URL is not already being processed
                if (((QueueMetadata) queue).isHeld(url, now)) {
                    continue;
                }

                Payload payload = _cache.get(new Key(queueID.toString(), url));
                // might have disappeared since
                if (payload == null) continue;

                // too early for it?
                if (payload.nextFetchDate > now) {
                    // they should be sorted by date no need to go further
                    return alreadySent;
                }

                // this one is good to go
                try {
                    responseObserver.onNext(URLInfo.parseFrom(payload.payload));

                    // mark it as not processable for N secs
                    ((QueueMetadata) queue).holdUntil(url, now + secsUntilRequestable);

                    alreadySent++;
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Caught unlikely error ", e);
                }
            }
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        return alreadySent;
    }

    @Override
    public void deleteCrawl(
            crawlercommons.urlfrontier.Urlfrontier.DeleteCrawlMessage crawlID,
            io.grpc.stub.StreamObserver<crawlercommons.urlfrontier.Urlfrontier.Long>
                    responseObserver) {

        long total = 0;

        final String normalisedCrawlID = CrawlID.normaliseCrawlID(crawlID.getValue());

        final Set<QueueWithinCrawl> toDelete = new HashSet<>();

        synchronized (queues) {

            // find the crawlIDs
            QueueWithinCrawl[] array = queues.keySet().toArray(new QueueWithinCrawl[0]);
            Arrays.sort(array);

            for (QueueWithinCrawl prefixed_queue : array) {
                boolean samePrefix = prefixed_queue.getCrawlid().equals(normalisedCrawlID);
                if (samePrefix) {
                    toDelete.add(prefixed_queue);
                }
            }

            ignite.destroyCache(URLCacheNamePrefix + normalisedCrawlID);

            for (QueueWithinCrawl quid : toDelete) {
                if (queuesBeingDeleted.contains(quid)) {
                    continue;
                } else {
                    queuesBeingDeleted.put(quid, quid);
                }

                QueueInterface q = queues.remove(quid);
                total += q.countActive();
                total += q.getCountCompleted();

                // remove at the global level
                globalQueueCache.remove(quid.toString());

                queuesBeingDeleted.remove(quid);
            }
        }
        responseObserver.onNext(
                crawlercommons.urlfrontier.Urlfrontier.Long.newBuilder().setValue(total).build());
        responseObserver.onCompleted();
    }

    /**
     *
     *
     * <pre>
     * * Delete the queue based on the key in parameter *
     * </pre>
     */
    @Override
    public int deleteLocalQueue(final QueueWithinCrawl qc) {

        int sizeQueue = 0;

        // if the queue is unknown or already being deleted
        if (!queues.containsKey(qc) || queuesBeingDeleted.contains(qc)) {
            return sizeQueue;
        }

        queuesBeingDeleted.put(qc, qc);

        IgniteCache<Key, Payload> URLCache = createOrGetCacheForCrawlID(qc.getCrawlid());

        Query<Entry<Key, Payload>> qry = new TextQuery<>(Payload.class, qc.toString());
        // the content for the queues should only be local anyway
        qry.setLocal(true);

        try (QueryCursor<Entry<Key, Payload>> cur = URLCache.query(qry)) {
            for (Entry<Key, Payload> entry : cur) {
                URLCache.remove(entry.getKey());
            }
        }

        QueueInterface q = queues.remove(qc);
        sizeQueue += q.countActive();
        sizeQueue += q.getCountCompleted();

        queuesBeingDeleted.remove(qc);

        // remove at the global level
        globalQueueCache.remove(qc.toString());

        return sizeQueue;
    }

    @Override
    protected long deleteLocalCrawl(String normalisedCrawlID) {
        final Set<QueueWithinCrawl> toDelete = new HashSet<>();

        long total = 0;

        synchronized (queues) {
            // find the crawlIDs
            QueueWithinCrawl[] array = queues.keySet().toArray(new QueueWithinCrawl[0]);
            Arrays.sort(array);

            for (QueueWithinCrawl prefixed_queue : array) {
                boolean samePrefix = prefixed_queue.getCrawlid().equals(normalisedCrawlID);
                if (samePrefix) {
                    toDelete.add(prefixed_queue);
                }
            }

            ignite.destroyCache(URLCacheNamePrefix + normalisedCrawlID);

            for (QueueWithinCrawl quid : toDelete) {
                if (queuesBeingDeleted.contains(quid)) {
                    continue;
                } else {
                    queuesBeingDeleted.put(quid, quid);
                }

                QueueInterface q = queues.remove(quid);
                total += q.countActive();
                total += q.getCountCompleted();

                // remove at the global level
                globalQueueCache.remove(quid.toString());

                queuesBeingDeleted.remove(quid);
            }
        }
        return total;
    }
}
