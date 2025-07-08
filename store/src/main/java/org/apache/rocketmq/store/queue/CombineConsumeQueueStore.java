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

package org.apache.rocketmq.store.queue;

import com.alibaba.fastjson.JSON;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.CheckRocksdbCqWriteResult;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.StoreType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.exception.StoreException;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombineConsumeQueueStore implements ConsumeQueueStoreInterface {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger BROKER_LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final DefaultMessageStore messageStore;
    private final MessageStoreConfig messageStoreConfig;

    // Inner consume queue store.
    private final LinkedList<AbstractConsumeQueueStore> innerConsumeQueueStoreList = new LinkedList<>();
    private final ConsumeQueueStore consumeQueueStore;
    private final RocksDBConsumeQueueStore rocksDBConsumeQueueStore;

    // current read consume queue store.
    private final AbstractConsumeQueueStore currentReadStore;
    // consume queue store for assign offset and increase offset.
    private final AbstractConsumeQueueStore assignOffsetStore;


    /**
     * ConsumeQueueStore recovers through commitlog dispatch, so it needs to search which file in the commitLog to
     * start recovery from. It might not be possible for all inner consumeQueueStores in CombineConsumeQueueStore to
     * fully recover (for example, a newly consumeQueueStore needs to start dispatch from the first file, which
     * could be very time-consuming).
     * <p>
     * However, we need to ensure that assignOffsetStore can be fully recovered to guarantee the correctness of
     * commitlog. When assignOffsetStore can be fully recovered but other stores cannot, we need to use
     * extraSearchCommitLogFilesForRecovery to control whether to continue searching forward for positions that might
     * satisfy the recovery of other stores.
     */

    private final AtomicInteger extraSearchCommitLogFilesForRecovery;

    public CombineConsumeQueueStore(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
        this.messageStoreConfig = messageStore.getMessageStoreConfig();
        extraSearchCommitLogFilesForRecovery =
            new AtomicInteger(messageStoreConfig.getCombineCQMaxExtraSearchCommitLogFiles());

        Set<StoreType> loadingConsumeQueueTypeSet = StoreType.fromString(messageStoreConfig.getCombineCQLoadingCQTypes());
        if (loadingConsumeQueueTypeSet.isEmpty()) {
            throw new IllegalArgumentException("CombineConsumeQueueStore loadingCQTypes is empty");
        }

        if (loadingConsumeQueueTypeSet.contains(StoreType.DEFAULT)) {
            this.consumeQueueStore = new ConsumeQueueStore(messageStore);
            this.innerConsumeQueueStoreList.addFirst(consumeQueueStore);
        } else {
            this.consumeQueueStore = null;
        }

        if (loadingConsumeQueueTypeSet.contains(StoreType.DEFAULT_ROCKSDB)) {
            this.rocksDBConsumeQueueStore = new RocksDBConsumeQueueStore(messageStore);
            this.innerConsumeQueueStoreList.addFirst(rocksDBConsumeQueueStore);
        } else {
            this.rocksDBConsumeQueueStore = null;
        }

        if (innerConsumeQueueStoreList.isEmpty()) {
            throw new IllegalArgumentException("CombineConsumeQueueStore loadingCQTypes is empty");
        }

        assignOffsetStore = getInnerStoreByString(messageStoreConfig.getCombineAssignOffsetCQType());
        if (assignOffsetStore == null) {
            log.error("CombineConsumeQueueStore chooseAssignOffsetStore fail, config={}",
                messageStoreConfig.getCombineAssignOffsetCQType());
            throw new IllegalArgumentException("CombineConsumeQueue chooseAssignOffsetStore fail");
        }

        currentReadStore = getInnerStoreByString(messageStoreConfig.getCombineCQPreferCQType());
        if (currentReadStore == null) {
            log.error("CombineConsumeQueueStore choosePreferCQ fail, config={}",
                messageStoreConfig.getCombineCQPreferCQType());
            throw new IllegalArgumentException("CombineConsumeQueue choosePreferCQ fail");
        }

        log.info("CombineConsumeQueueStore init, consumeQueueStoreList={}, currentReadStore={}, assignOffsetStore={}",
            innerConsumeQueueStoreList, currentReadStore.getClass().getSimpleName(), assignOffsetStore.getClass().getSimpleName());
    }

    @Override
    public boolean load() {
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            if (!store.load()) {
                log.error("CombineConsumeQueueStore load fail, loadType={}", store.getClass().getSimpleName());
                return false;
            }
        }
        log.info("CombineConsumeQueueStore load success");
        return true;
    }

    @Override
    public void recover(boolean concurrently) throws RocksDBException {
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            store.recover(concurrently);
        }
        log.info("CombineConsumeQueueStore recover success, concurrently={}", concurrently);
    }

    @Override
    public boolean isMappedFileMatchedRecover(long phyOffset, long storeTimestamp,
        boolean recoverNormally) throws RocksDBException {
        // make sure assignOffsetStore can be fully recovered
        if (!assignOffsetStore.isMappedFileMatchedRecover(phyOffset, storeTimestamp, recoverNormally)) {
            return false;
        }

        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            if (store == assignOffsetStore || store.isMappedFileMatchedRecover(phyOffset, storeTimestamp, recoverNormally)) {
                continue;
            }
            // if other store is not matched for fully recovery, extraSearchCommitLogFilesForRecovery will minus 1
            if (extraSearchCommitLogFilesForRecovery.getAndDecrement() <= 0) {
                // extraSearchCommitLogFilesForRecovery <= 0, only can read from assignOffsetStore
                if (assignOffsetStore != currentReadStore) {
                    log.error("CombineConsumeQueueStore currentReadStore not satisfied readable conditions, assignOffsetStore={}, currentReadStore={}",
                        assignOffsetStore.getClass().getSimpleName(), currentReadStore.getClass().getSimpleName());
                    throw new IllegalArgumentException(store.getClass().getSimpleName() + " not satisfied readable conditions, only can read from " + assignOffsetStore.getClass().getSimpleName());
                }
                log.warn("CombineConsumeQueueStore can not recover all inner store, maybe some inner store start haven’t started before, store={}",
                    store.getClass().getSimpleName());
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public long getDispatchFromPhyOffset() {
        long dispatchFromPhyOffset = assignOffsetStore.getDispatchFromPhyOffset();
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            if (store == assignOffsetStore) {
                continue;
            }
            if (store.getDispatchFromPhyOffset() < dispatchFromPhyOffset) {
                dispatchFromPhyOffset = store.getDispatchFromPhyOffset();
            }
        }
        return dispatchFromPhyOffset;
    }

    @Override
    public void start() {
        boolean success = false;
        try {
            success = verifyAndInitOffsetForAllStore(true);
        } catch (RocksDBException e) {
            log.error("CombineConsumeQueueStore checkAssignOffsetStore fail", e);
        }

        if (!success && assignOffsetStore != currentReadStore) {
            log.error("CombineConsumeQueueStore currentReadStore not satisfied readable conditions, " +
                    "checkAssignOffsetResult={}, assignOffsetStore={}, currentReadStore={}",
                success, assignOffsetStore.getClass().getSimpleName(), currentReadStore.getClass().getSimpleName());
            throw new RuntimeException("CombineConsumeQueueStore currentReadStore not satisfied readable conditions");
        }

        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            store.start();
        }
    }

    public boolean verifyAndInitOffsetForAllStore(boolean initializeOffset) throws RocksDBException {
        if (innerConsumeQueueStoreList.size() <= 1) {
            return true;
        }

        boolean result = true;
        long minPhyOffset = this.messageStore.getCommitLog().getMinOffset();
        // for each topic and queueId in assignOffsetStore
        for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> entry : assignOffsetStore.getConsumeQueueTable().entrySet()) {
            for (Map.Entry<Integer, ConsumeQueueInterface> entry0 : entry.getValue().entrySet()) {
                String topic = entry.getKey();
                int queueId = entry0.getKey();
                long maxOffsetInAssign = entry0.getValue().getMaxOffsetInQueue();

                for (AbstractConsumeQueueStore abstractConsumeQueueStore : innerConsumeQueueStoreList) {
                    // skip compare self
                    if (abstractConsumeQueueStore == assignOffsetStore) {
                        continue;
                    }

                    ConsumeQueueInterface queue = abstractConsumeQueueStore.findOrCreateConsumeQueue(topic, queueId);
                    long maxOffset0 = queue.getMaxOffsetInQueue();

                    if (maxOffsetInAssign == maxOffset0 || maxOffsetInAssign <= 0 && maxOffset0 <= 0) {
                        continue;
                    }

                    if (maxOffset0 > 0) {
                        log.error("CombineConsumeQueueStore checkAssignOffsetStore fail, topic={}, queueId={}, maxOffsetInAssign={}, otherCQ={}, maxOffset0={}",
                            topic, queueId, maxOffsetInAssign, abstractConsumeQueueStore.getClass().getSimpleName(), maxOffset0);
                        result = false;
                    }

                    if (initializeOffset) {
                        queue.initializeWithOffset(maxOffsetInAssign, minPhyOffset);
                        log.info("CombineConsumeQueueStore initialize offset in queue, topic={}, queueId={}, maxOffsetInAssign={}, otherCQ={}, maxOffset0={}, maxOffsetNew={}",
                            topic, queueId, maxOffsetInAssign, abstractConsumeQueueStore.getClass().getSimpleName(), maxOffset0, queue.getMaxOffsetInQueue());
                    }
                }
            }
        }
        return result;
    }

    @Override
    public boolean shutdown() {
        boolean result = true;
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            if (!store.shutdown()) {
                result = false;
            }
        }
        return result;
    }

    @Override
    public void destroy(boolean loadAfterDestroy) {
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            store.destroy(loadAfterDestroy);
        }
    }

    @Override
    public boolean deleteTopic(String topic) {
        boolean result = false;
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            if (store.deleteTopic(topic)) {
                result = true;
            }
        }
        return result;
    }

    @Override
    public void flush() throws StoreException {
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            store.flush();
        }
    }

    @Override
    public void cleanExpired(long minCommitLogOffset) {
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            store.cleanExpired(minCommitLogOffset);
        }
    }

    @Override
    public void checkSelf() {
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            store.checkSelf();
        }

        if (messageStoreConfig.isCombineCQEnableCheckSelf()) {
            try {
                verifyAndInitOffsetForAllStore(false);
            } catch (RocksDBException e) {
                log.error("CombineConsumeQueueStore checkAssignOffsetStore fail in checkSelf", e);
            }
            CheckRocksdbCqWriteResult checkResult = doCheckCqWriteProgress(null, System.currentTimeMillis() - 10 * 60 * 1000, StoreType.DEFAULT, StoreType.DEFAULT_ROCKSDB);
            BROKER_LOG.info("checkRocksdbCqWriteProgress result: {}", JSON.toJSONString(checkResult));
        }
    }

    @Override
    public void truncateDirty(long offsetToTruncate) throws RocksDBException {
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            store.truncateDirty(offsetToTruncate);
        }
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) throws RocksDBException {
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            store.putMessagePositionInfoWrapper(request);
        }
    }

    @Override
    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
        return currentReadStore.getConsumeQueueTable();
    }

    @Override
    public void assignQueueOffset(MessageExtBrokerInner msg) throws RocksDBException {
        assignOffsetStore.assignQueueOffset(msg);
    }

    @Override
    public void increaseQueueOffset(MessageExtBrokerInner msg, short messageNum) {
        assignOffsetStore.increaseQueueOffset(msg, messageNum);
    }

    @Override
    public void increaseLmqOffset(String topic, int queueId, short delta) throws ConsumeQueueException {
        assignOffsetStore.increaseLmqOffset(topic, queueId, delta);
    }

    @Override
    public long getLmqQueueOffset(String topic, int queueId) throws ConsumeQueueException {
        return assignOffsetStore.getLmqQueueOffset(topic, queueId);
    }

    @Override
    public void recoverOffsetTable(long minPhyOffset) {
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            store.recoverOffsetTable(minPhyOffset);
        }
    }

    @Override
    public Long getMaxOffset(String topic, int queueId) throws ConsumeQueueException {
        return currentReadStore.getMaxOffset(topic, queueId);
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) throws RocksDBException {
        return currentReadStore.getMinOffsetInQueue(topic, queueId);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp,
        BoundaryType boundaryType) throws RocksDBException {
        return currentReadStore.getOffsetInQueueByTime(topic, queueId, timestamp, boundaryType);
    }

    @Override
    public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
        return currentReadStore.findOrCreateConsumeQueue(topic, queueId);
    }

    @Override
    public ConsumeQueueInterface getConsumeQueue(String topic, int queueId) {
        return currentReadStore.getConsumeQueue(topic, queueId);
    }

    @Override
    public long getTotalSize() {
        long result = 0;
        for (AbstractConsumeQueueStore store : innerConsumeQueueStoreList) {
            result += store.getTotalSize();
        }
        return result;
    }

    public RocksDBConsumeQueueStore getRocksDBConsumeQueueStore() {
        return rocksDBConsumeQueueStore;
    }

    @VisibleForTesting
    public ConsumeQueueStore getConsumeQueueStore() {
        return consumeQueueStore;
    }

    @VisibleForTesting
    public AbstractConsumeQueueStore getCurrentReadStore() {
        return currentReadStore;
    }

    @VisibleForTesting
    public AbstractConsumeQueueStore getAssignOffsetStore() {
        return assignOffsetStore;
    }

    public CheckRocksdbCqWriteResult doCheckCqWriteProgress(String requestTopic, long checkStoreTime,
        StoreType baseStoreType, StoreType compareStoreType) {
        CheckRocksdbCqWriteResult result = new CheckRocksdbCqWriteResult();
        AbstractConsumeQueueStore baseStore = getInnerStoreByStoreType(baseStoreType);
        AbstractConsumeQueueStore compareStore = getInnerStoreByStoreType(compareStoreType);

        if (baseStore == null || compareStore == null) {
            result.setCheckResult("baseStore or compareStore is null, no need check");
            result.setCheckStatus(CheckRocksdbCqWriteResult.CheckStatus.CHECK_OK.getValue());
            return result;
        }

        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> cqTable = baseStore.getConsumeQueueTable();
        StringBuilder diffResult = new StringBuilder();
        try {
            if (StringUtils.isNotBlank(requestTopic)) {
                boolean checkResult = processConsumeQueuesForTopic(cqTable.get(requestTopic), requestTopic, compareStore, diffResult, true, checkStoreTime);
                result.setCheckResult(diffResult.toString());
                result.setCheckStatus(checkResult ? CheckRocksdbCqWriteResult.CheckStatus.CHECK_OK.getValue() : CheckRocksdbCqWriteResult.CheckStatus.CHECK_NOT_OK.getValue());
                return result;
            }
            int successNum = 0;
            int checkSize = 0;
            for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> topicEntry : cqTable.entrySet()) {
                boolean checkResult = processConsumeQueuesForTopic(topicEntry.getValue(), topicEntry.getKey(), compareStore, diffResult, false, checkStoreTime);
                successNum += checkResult ? 1 : 0;
                checkSize++;
            }
            // check all topic finish, all topic is ready, checkSize: 100, currentQueueNum: 110      -> ready  (The currentQueueNum means when we do checking, new topics are added.)
            // check all topic finish, success/all : 89/100, currentQueueNum: 110                    -> not ready
            boolean checkReady = successNum == checkSize;
            String checkResultString = checkReady ? String.format("all topic is ready, checkSize: %s, currentQueueNum: %s", checkSize, cqTable.size()) :
                String.format("success/all : %s/%s, currentQueueNum: %s", successNum, checkSize, cqTable.size());
            diffResult.append("check all topic finish, ").append(checkResultString);
            result.setCheckResult(diffResult.toString());
            result.setCheckStatus(checkReady ? CheckRocksdbCqWriteResult.CheckStatus.CHECK_OK.getValue() : CheckRocksdbCqWriteResult.CheckStatus.CHECK_NOT_OK.getValue());
        } catch (Exception e) {
            log.error("CheckRocksdbCqWriteProgressCommand error", e);
            result.setCheckResult(e.getMessage() + Arrays.toString(e.getStackTrace()));
            result.setCheckStatus(CheckRocksdbCqWriteResult.CheckStatus.CHECK_ERROR.getValue());
        }
        return result;
    }

    private boolean processConsumeQueuesForTopic(ConcurrentMap<Integer, ConsumeQueueInterface> queueMap, String topic,
        AbstractConsumeQueueStore abstractConsumeQueueStore, StringBuilder diffResult, boolean printDetail,
        long checkpointByStoreTime) {
        boolean processResult = true;
        for (Map.Entry<Integer, ConsumeQueueInterface> queueEntry : queueMap.entrySet()) {
            Integer queueId = queueEntry.getKey();
            ConsumeQueueInterface baseCQ = queueEntry.getValue();
            ConsumeQueueInterface compareCQ = abstractConsumeQueueStore.findOrCreateConsumeQueue(topic, queueId);
            if (printDetail) {
                String format = String.format("[topic: %s, queue:  %s] \n  kvEarliest : %s |  kvLatest : %s \n fileEarliest: %s | fileEarliest: %s ",
                    topic, queueId, compareCQ.getEarliestUnit(), compareCQ.getLatestUnit(), baseCQ.getEarliestUnit(), baseCQ.getLatestUnit());
                diffResult.append(format).append("\n");
            }

            long minOffsetByTime = 0L;
            try {
                minOffsetByTime = abstractConsumeQueueStore.getOffsetInQueueByTime(topic, queueId, checkpointByStoreTime, BoundaryType.UPPER);
            } catch (Exception e) {
                // ignore
            }
            long minOffsetInQueue = compareCQ.getMinOffsetInQueue();
            long checkFrom = Math.max(minOffsetInQueue, minOffsetByTime);
            long checkTo = baseCQ.getMaxOffsetInQueue() - 1;
            /*
                                                            checkTo(maxOffsetInQueue - 1)
                                                                        v
        baseCQ   +------------------------------------------------------+
        compareCQ        +----------------------------------------------+
                         ^                ^
                   minOffsetInQueue   minOffsetByTime
                                          ^
                        checkFrom = max(minOffsetInQueue, minOffsetByTime)
             */
            // The latest message is earlier than the check time
            Pair<CqUnit, Long> fileLatestCq = baseCQ.getCqUnitAndStoreTime(checkTo);
            if (fileLatestCq != null) {
                if (fileLatestCq.getObject2() < checkpointByStoreTime) {
                    continue;
                }
            }
            for (long i = checkFrom; i <= checkTo; i++) {
                Pair<CqUnit, Long> baseCqUnit = baseCQ.getCqUnitAndStoreTime(i);
                Pair<CqUnit, Long> compareCqUnit = compareCQ.getCqUnitAndStoreTime(i);
                if (baseCqUnit == null || compareCqUnit == null || !checkCqUnitEqual(compareCqUnit.getObject1(), baseCqUnit.getObject1())) {
                    log.error(String.format("[topic: %s, queue: %s, offset: %s] \n file : %s  \n  kv : %s \n",
                        topic, queueId, i, compareCqUnit != null ? compareCqUnit.getObject1() : "null", baseCqUnit != null ? baseCqUnit.getObject1() : "null"));
                    processResult = false;
                    break;
                }
            }
        }
        return processResult;
    }

    private boolean checkCqUnitEqual(CqUnit cqUnit1, CqUnit cqUnit2) {
        if (cqUnit1.getQueueOffset() != cqUnit2.getQueueOffset()) {
            return false;
        }
        if (cqUnit1.getSize() != cqUnit2.getSize()) {
            return false;
        }
        if (cqUnit1.getPos() != cqUnit2.getPos()) {
            return false;
        }
        if (cqUnit1.getBatchNum() != cqUnit2.getBatchNum()) {
            return false;
        }
        return cqUnit1.getTagsCode() == cqUnit2.getTagsCode();
    }

    private AbstractConsumeQueueStore getInnerStoreByString(String storeTypeString) {
        if (StoreType.DEFAULT.getStoreType().equalsIgnoreCase(storeTypeString)) {
            return consumeQueueStore;
        } else if (StoreType.DEFAULT_ROCKSDB.getStoreType().equalsIgnoreCase(storeTypeString)) {
            return rocksDBConsumeQueueStore;
        } else {
            return null;
        }
    }

    private AbstractConsumeQueueStore getInnerStoreByStoreType(StoreType storeType) {
        switch (storeType) {
            case DEFAULT:
                return consumeQueueStore;
            case DEFAULT_ROCKSDB:
                return rocksDBConsumeQueueStore;
            default:
                return null;
        }
    }
}
