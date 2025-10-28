/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.index.rocksdb;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage;
import org.rocksdb.RocksDB;
import static org.apache.rocketmq.common.MixAll.dealTimeToHourStamps;

public class IndexRocksDBStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final int DEFAULT_CAPACITY = 100000;
    private static final int BATCH_SIZE = 1000;
    private static final Set<String> INDEX_TYPE_SET = new HashSet<>();
    static {
        INDEX_TYPE_SET.add(MessageConst.INDEX_KEY_TYPE);
        INDEX_TYPE_SET.add(MessageConst.INDEX_TAG_TYPE);
        INDEX_TYPE_SET.add(MessageConst.INDEX_UNIQUE_TYPE);
    }
    private static final int INITIAL = 0, RUNNING = 1, SHUTDOWN = 2;
    private volatile int state = INITIAL;

    private final MessageStore messageStore;
    private final MessageStoreConfig storeConfig;
    private final MessageRocksDBStorage messageRocksDBStorage;
    private volatile long lastDeleteIndexTime = 0L;
    private IndexBuildService indexBuildService;
    private BlockingQueue<IndexRocksDBRecord> originIndexMsgQueue;

    public IndexRocksDBStore(MessageStore messageStore) {
        this.messageStore = messageStore;
        this.storeConfig = messageStore.getMessageStoreConfig();
        this.messageRocksDBStorage = messageStore.getMessageRocksDBStorage();
        if (this.storeConfig.isIndexRocksDBEnable()) {
            this.initAndStart();
        }
    }

    private void initAndStart() {
        if (this.state == RUNNING) {
            return;
        }
        this.indexBuildService = new IndexBuildService();
        this.originIndexMsgQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
        this.indexBuildService.start();
        this.state = RUNNING;
        Long lastOffsetPy = messageRocksDBStorage.getLastOffsetPy(RocksDB.DEFAULT_COLUMN_FAMILY);
        log.info("IndexRocksDBStore start success, lastOffsetPy: {}", lastOffsetPy);
    }

    public void shutdown() {
        if (this.state != RUNNING || this.state == SHUTDOWN) {
            return;
        }
        if (null != this.indexBuildService) {
            this.indexBuildService.shutdown();
        }
        this.state = SHUTDOWN;
        log.info("IndexRocksDBStore shutdown success");
    }

    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long beginTime, long endTime, String indexType, String lastKey) {
        if (StringUtils.isEmpty(topic) || StringUtils.isEmpty(key) || maxNum <= 0 || beginTime < 0L || endTime <= 0L || beginTime > endTime || !StringUtils.isEmpty(indexType) && !INDEX_TYPE_SET.contains(indexType)) {
            logError.error("IndexRocksDBStore queryOffset param error, topic: {}, key: {}, maxNum: {}, beginTime: {}, endTime: {}, indexType: {}, lastKey: {}", topic, key, maxNum, beginTime, endTime, indexType, lastKey);
            return null;
        }
        if (beginTime == 0L || Long.MAX_VALUE == endTime) {
            endTime = System.currentTimeMillis();
            beginTime = endTime - TimeUnit.DAYS.toMillis(storeConfig.getMaxRocksDBIndexQueryDays());
        }
        if ((endTime - beginTime) > (TimeUnit.DAYS.toMillis(storeConfig.getMaxRocksDBIndexQueryDays()))) {
            logError.error("IndexRocksDBStore queryOffset index in rocksdb, can not query more than: {} days", storeConfig.getMaxRocksDBIndexQueryDays());
            return null;
        }
        long lastUpdateTime = 0L;
        long lastOffsetPy = 0L;
        maxNum = Math.min(maxNum, this.storeConfig.getMaxMsgsNumBatch());
        List<Long> phyOffsets = null;
        try {
            lastUpdateTime = messageRocksDBStorage.getLastStoreTimeStampForIndex(RocksDB.DEFAULT_COLUMN_FAMILY);
            lastOffsetPy = messageRocksDBStorage.getLastOffsetPy(RocksDB.DEFAULT_COLUMN_FAMILY);
            //compact old client
            if (StringUtils.isEmpty(indexType)) {
                phyOffsets = messageRocksDBStorage.queryOffsetForIndex(RocksDB.DEFAULT_COLUMN_FAMILY, topic, MessageConst.INDEX_KEY_TYPE, key, beginTime, endTime, maxNum, null);
                if (CollectionUtils.isEmpty(phyOffsets)) {
                    phyOffsets = messageRocksDBStorage.queryOffsetForIndex(RocksDB.DEFAULT_COLUMN_FAMILY, topic, MessageConst.INDEX_UNIQUE_TYPE, key, beginTime, endTime, maxNum, null);
                }
            } else {
                phyOffsets = messageRocksDBStorage.queryOffsetForIndex(RocksDB.DEFAULT_COLUMN_FAMILY, topic, indexType, key, beginTime, endTime, maxNum, lastKey);
            }
        } catch (Exception e) {
            logError.error("IndexRocksDBStore queryOffset error, topic: {}, key: {}, maxNum: {}, beginTime: {}, endTime: {}, error: {}", topic, key, maxNum, beginTime, endTime, e.getMessage());
        }
        return new QueryOffsetResult(phyOffsets, lastUpdateTime, lastOffsetPy);
    }

    public void buildIndex(DispatchRequest dispatchRequest) {
        if (null == dispatchRequest || dispatchRequest.getCommitLogOffset() < 0L || dispatchRequest.getMsgSize() <= 0 || state != RUNNING || null == this.originIndexMsgQueue) {
            logError.error("IndexRocksDBStore buildIndex error, dispatchRequest: {}, state: {}, originIndexMsgQueue: {}", dispatchRequest, state, originIndexMsgQueue);
            return;
        }
        try {
            long reqOffsetPy = dispatchRequest.getCommitLogOffset();
            long endOffsetPy = messageRocksDBStorage.getLastOffsetPy(RocksDB.DEFAULT_COLUMN_FAMILY);
            if (reqOffsetPy < endOffsetPy) {
                if (System.currentTimeMillis() % 1000 == 0) {
                    log.warn("IndexRocksDBStore recover buildIndex, but ignore, build index offset reqOffsetPy: {}, endOffsetPy: {}", reqOffsetPy, endOffsetPy);
                }
                return;
            }
            final int tranType = MessageSysFlag.getTransactionValue(dispatchRequest.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }
            String topic = dispatchRequest.getTopic();
            String uniqKey = dispatchRequest.getUniqKey();
            long storeTime = dispatchRequest.getStoreTimestamp();
            if (StringUtils.isEmpty(topic) || StringUtils.isEmpty(uniqKey) || storeTime <= 0L || reqOffsetPy < 0L) {
                return;
            }
            String keys = dispatchRequest.getKeys();
            if (!StringUtils.isEmpty(keys)) {
                String[] keySplit = keys.split(MessageConst.KEY_SEPARATOR);
                if (keySplit.length > 0) {
                    Set<String> keySet = Arrays.stream(keySplit).filter(i -> !StringUtils.isEmpty(i)).collect(Collectors.toSet());
                    for (String key : keySet) {
                        try {
                            while (!originIndexMsgQueue.offer(new IndexRocksDBRecord(topic, key, null, storeTime, uniqKey, reqOffsetPy), 3, TimeUnit.SECONDS)) {
                                if (System.currentTimeMillis() % 1000 == 0) {
                                    logError.error("IndexRocksDBStore buildIndex keys error, topic: {}, key: {}, storeTime: {}, uniqKey: {}, reqOffsetPy: {}", topic, key, storeTime, uniqKey, reqOffsetPy);
                                }
                            }
                        } catch (Exception e) {
                            logError.error("IndexRocksDBStore buildIndex keys error, key: {}, uniqKey: {}, topic: {}, error: {}", key, uniqKey, topic, e.getMessage());
                        }
                    }
                }
            }
            Map<String, String> propertiesMap = dispatchRequest.getPropertiesMap();
            if (null != propertiesMap && propertiesMap.containsKey(MessageConst.PROPERTY_TAGS)) {
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (!StringUtils.isEmpty(tags)) {
                    try {
                        while (!originIndexMsgQueue.offer(new IndexRocksDBRecord(topic, null, tags, storeTime, uniqKey, reqOffsetPy), 3, TimeUnit.SECONDS)) {
                            if (System.currentTimeMillis() % 1000 == 0) {
                                logError.error("IndexRocksDBStore buildIndex offer tags error, topic: {}, tags: {}, storeTime: {}, uniqKey: {}, reqOffsetPy: {}", topic, tags, storeTime, uniqKey, reqOffsetPy);
                            }
                        }
                    } catch (Exception e) {
                        logError.error("IndexRocksDBStore buildIndex tags error, tags: {}, uniqKey: {}, topic: {}, error: {}", tags, uniqKey, topic, e.getMessage());
                    }
                }
            }
            try {
                while (!originIndexMsgQueue.offer(new IndexRocksDBRecord(topic, null, null, storeTime, uniqKey, reqOffsetPy), 3, TimeUnit.SECONDS)) {
                    if (System.currentTimeMillis() % 1000 == 0) {
                        logError.error("IndexRocksDBStore buildIndex uniqKey error, topic: {}, storeTime: {}, uniqKey: {}, reqOffsetPy: {}", topic, storeTime, uniqKey, reqOffsetPy);
                    }
                }
            } catch (Exception e) {
                logError.error("IndexRocksDBStore buildIndex uniqKey error: {}", e.getMessage());
            }
        } catch (Exception e) {
            logError.error("IndexRocksDBStore buildIndex error: {}", e.getMessage());
        }
    }

    public void deleteExpiredIndex() {
        try {
            MappedFile mappedFile = messageStore.getCommitLog().getEarliestMappedFile();
            if (null == mappedFile) {
                logError.info("IndexRocksDBStore deleteExpiredIndex mappedFile is null");
                return;
            }
            File file = mappedFile.getFile();
            if (null == file || StringUtils.isEmpty(file.getAbsolutePath())) {
                logError.info("IndexRocksDBStore deleteExpiredIndex error, file is null");
                return;
            }
            Path path = Paths.get(file.getAbsolutePath());
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
            long deleteIndexFileTime = attrs.creationTime().toMillis() - TimeUnit.HOURS.toMillis(1);
            long desDeleteTimeHour = dealTimeToHourStamps(deleteIndexFileTime);
            if (desDeleteTimeHour != lastDeleteIndexTime) {
                messageRocksDBStorage.deleteRecordsForIndex(RocksDB.DEFAULT_COLUMN_FAMILY, desDeleteTimeHour, 168);
                lastDeleteIndexTime = desDeleteTimeHour;
            } else {
                log.info("IndexRocksDBStore ignore this delete, lastDeleteIndexTime: {}, desDeleteTimeHour: {}", lastDeleteIndexTime, desDeleteTimeHour);
            }
        } catch (Exception e) {
            logError.error("IndexRocksDBStore deleteExpiredIndex rocksdb error: {}", e.getMessage());
        }
    }

    public boolean isMappedFileMatchedRecover(long phyOffset) {
        if (!storeConfig.isIndexRocksDBEnable()) {
            return true;
        }
        Long lastOffsetPy = messageRocksDBStorage.getLastOffsetPy(RocksDB.DEFAULT_COLUMN_FAMILY);
        log.info("index isMappedFileMatchedRecover lastOffsetPy: {}", lastOffsetPy);
        if (null != lastOffsetPy && phyOffset <= lastOffsetPy) {
            log.info("isMappedFileMatchedRecover IndexRocksDBStore recover form this offset, phyOffset: {}, lastOffsetPy: {}", phyOffset, lastOffsetPy);
            return true;
        }
        return false;
    }

    public void destroy() {}

    private String getServiceThreadName() {
        String brokerIdentifier = "";
        if (this.messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore = (DefaultMessageStore) IndexRocksDBStore.this.messageStore;
            if (messageStore.getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = messageStore.getBrokerConfig().getIdentifier();
            }
        }
        return brokerIdentifier;
    }

    private class IndexBuildService extends ServiceThread {
        private final Logger log = IndexRocksDBStore.log;
        private List<IndexRocksDBRecord> irs;
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service start");
            irs = new ArrayList<>(BATCH_SIZE);
            while (!this.isStopped() || !originIndexMsgQueue.isEmpty()) {
                try {
                    pollAndPutIndexRequest();
                } catch (Exception e) {
                    irs.clear();
                    logError.error("IndexRocksDBStore error occurred in: {}, error: {}", getServiceName(), e.getMessage());
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        private void pollAndPutIndexRequest() {
            pollIndexRecord();
            if (CollectionUtils.isEmpty(irs)) {
                return;
            }
            try {
                messageRocksDBStorage.writeRecordsForIndex(RocksDB.DEFAULT_COLUMN_FAMILY, irs);
            } catch (Exception e) {
                logError.error("IndexRocksDBStore IndexBuildService pollAndPutIndexRequest error: {}", e.getMessage());
            }
            irs.clear();
        }

        private void pollIndexRecord() {
            try {
                IndexRocksDBRecord firstReq = originIndexMsgQueue.poll(100, TimeUnit.MILLISECONDS);
                if (null != firstReq) {
                    irs.add(firstReq);
                    while (true) {
                        IndexRocksDBRecord tmpReq = originIndexMsgQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (null == tmpReq) {
                            break;
                        }
                        irs.add(tmpReq);
                        if (irs.size() >= BATCH_SIZE) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logError.error("IndexRocksDBStore IndexBuildService error: {}", e.getMessage());
            }
        }
    }
}
