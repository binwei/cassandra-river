/*Copyright 2013, eBay Software Foundation
   Authored by Utkarsh Sengar
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
       http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package org.elasticsearch.river.cassandra;

import static org.elasticsearch.client.Requests.indexRequest;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadFactoryBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ScriptService;

public class CassandraRiver extends AbstractRiverComponent implements River {

    private final Client client;

    private ExecutorService threadExecutor;
    private volatile boolean closed;

    //Cassandra settings
    private final String hosts;
    private final String username;
    private final String password;

    private final String clusterName;
    private final String keyspace;
    private final String columnFamily;
    private final int batchSize;

    //Index settings
    private final String typeName;
    private final String indexName;
    static final String DEFAULT_UNIQUE_KEY = "id";

    private final int testRunRows;
    //Bulk processing with throttle
    private AtomicInteger totalSuccessActionsCount;
    private AtomicInteger pendingActionsCount;
    private long runStartTime;
    private volatile BulkProcessor bulkProcessor;

    @Inject
    protected CassandraRiver(RiverName riverName, RiverSettings riverSettings, Client client, ScriptService scriptService) {
        super(riverName, riverSettings);
        this.client = client;

        if (riverSettings.settings().containsKey("cassandra")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> cassandraSettings = (Map<String, Object>) settings.settings().get("cassandra");
            this.clusterName = XContentMapValues.nodeStringValue(cassandraSettings.get("cluster_name"), "DEFAULT_CLUSTER");
            this.keyspace = XContentMapValues.nodeStringValue(cassandraSettings.get("keyspace"), "DEFAULT_KS");
            this.columnFamily = XContentMapValues.nodeStringValue(cassandraSettings.get("column_family"), "DEFAULT_CF");
            this.batchSize = XContentMapValues.nodeIntegerValue(cassandraSettings.get("batch_size"), 1000);
            this.hosts = XContentMapValues.nodeStringValue(cassandraSettings.get("hosts"), "host1:9161,host2:9161");
            this.username = XContentMapValues.nodeStringValue(cassandraSettings.get("username"), "USERNAME");
            this.password = XContentMapValues.nodeStringValue(cassandraSettings.get("password"), "P$$WD");
            this.testRunRows = XContentMapValues.nodeIntegerValue(cassandraSettings.get("testRunRows"), -1);
        } else {
            /*
        	 * Set default values
        	 */
            this.clusterName = "DEFAULT_CLUSTER";
            this.keyspace = "DEFAULT_KS";
            this.columnFamily = "DEFAULT_CF";
            this.batchSize = 1000;
            this.hosts = "host1:9161,host2:9161";
            this.username = "USERNAME";
            this.password = "P$$WD";
            this.testRunRows = -1;
        }

        if (riverSettings.settings().containsKey("index")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            this.indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), "DEFAULT_INDEX_NAME");
            this.typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "DEFAULT_TYPE_NAME");

        } else {
            this.indexName = "DEFAULT_INDEX_NAME";
            this.typeName = "DEFAULT_TYPE_NAME";
        }
    }


    @Override
    public void start() {
        ThreadFactory daemonThreadFactory = new ThreadFactoryBuilder().setNameFormat("Queue-Indexer-thread-%d").setDaemon(false).build();
        threadExecutor = Executors.newFixedThreadPool(10, daemonThreadFactory);

        logger.info("Starting cassandra river");
        CassandraDB db = CassandraDB.getInstance(this.hosts, this.username, this.password, this.clusterName, this.keyspace);
        String start = "";

        totalSuccessActionsCount = new AtomicInteger(0);
        pendingActionsCount = new AtomicInteger(0);
        runStartTime = System.currentTimeMillis();

        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                logger.info("Inserting {} keys in ES", bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkItemResponses) {
                int currentTotal = totalSuccessActionsCount.addAndGet(bulkRequest.numberOfActions());
                pendingActionsCount.addAndGet(-bulkRequest.numberOfActions());
                logger.info("######CassandraRiver::Processing done with total {} actions after {} ms",
                        currentTotal, (System.currentTimeMillis() - runStartTime));
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable failure) {
                pendingActionsCount.addAndGet(-bulkRequest.numberOfActions());
                logger.warn("Error executing bulk", failure);
            }
        }).setBulkActions(batchSize).setConcurrentRequests(1).setFlushInterval(TimeValue.timeValueMillis(5000)).build();

        long cumulativeSleepTime = 0;
        while (true) {
            if (closed) {
                return;
            }

            if (testRunRows > 0 && totalSuccessActionsCount.get() > testRunRows) {
                logger.info("######CassandraRiver::done with dry run after writing total actions {}, after {} ms",
                        totalSuccessActionsCount.get(), (System.currentTimeMillis() - runStartTime));
                return;
            }

            if (pendingActionsCount.get() > 10000) {
                if (cumulativeSleepTime > 600 * 1000) { // stuck for more than 10 minutes
                    logger.info("######CassandraRiver::abort because throttling is stuck after writing total actions {}, after {} ms",
                            totalSuccessActionsCount.get(), (System.currentTimeMillis() - runStartTime));
                    return;
                }

                logger.info("######CassandraRiver::throttling with {} outstanding actions and {} cumulative sleep", pendingActionsCount.get(), cumulativeSleepTime);
                long sleepStart = System.currentTimeMillis();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                    if (closed) {
                        return;
                    }
                }
                cumulativeSleepTime += System.currentTimeMillis() - sleepStart;
                this.bulkProcessor.flush();
                continue;
            }

            cumulativeSleepTime = 0;

            CassandraCFData cassandraData = db.getCFData(columnFamily, start, 1000);
            if (null == cassandraData.start) {
                logger.info("######CassandraRiver::done with reading column family, writing total actions {}, after {} ms",
                        totalSuccessActionsCount.get(), (System.currentTimeMillis() - runStartTime));
                return;
            }

            start = cassandraData.start;
            pendingActionsCount.addAndGet(cassandraData.rowColumnMap.size());

            threadExecutor.execute(new Indexer(
                    this.typeName,
                    this.indexName,
                    cassandraData));
        }
    }


    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing cassandra river");
        closed = true;

        if (this.bulkProcessor != null) {
            this.bulkProcessor.close();
        }

        threadExecutor.shutdownNow();
    }

    private class Indexer implements Runnable {
        private final CassandraCFData keys;
        private final String typeName;
        private final String indexName;

        public Indexer(String typeName, String indexName, CassandraCFData keys) {
            this.typeName = typeName;
            this.indexName = indexName;
            this.keys = keys;
        }

        @Override
        public void run() {
            logger.info("Starting thread with {} keys", this.keys.rowColumnMap.size());
            if (closed) {
                return;
            }

            for (String key : this.keys.rowColumnMap.keySet()) {
                try {
                    bulkProcessor.add(indexRequest(this.indexName).type(this.typeName)
                            .id(key)
                            .source(this.keys.rowColumnMap.get(key)));
                } catch (Exception e) {
                    pendingActionsCount.decrementAndGet();
                    logger.error("failed to execute bulk indexing for " + key, e);
                }
            }
            logger.info("######CassandraRiver::run finished, {} pending actions", pendingActionsCount.get());
        }
    }
}