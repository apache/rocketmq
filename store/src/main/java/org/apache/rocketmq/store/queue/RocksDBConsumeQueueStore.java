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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.exception.StoreException;
import org.apache.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBatch;

public class RocksDBConsumeQueueStore extends AbstractConsumeQueueStore {
    private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final Logger ROCKSDB_LOG = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);

    private static final int DEFAULT_BYTE_BUFFER_CAPACITY = 16;

    public static final int MAX_KEY_LEN = 300;

    private final ScheduledExecutorService scheduledExecutorService;
    private final String storePath;

    /**
     * we use two tables with different ColumnFamilyHandle, called RocksDBConsumeQueueTable and RocksDBConsumeQueueOffsetTable.
     * 1.RocksDBConsumeQueueTable uses to store CqUnit[physicalOffset, msgSize, tagHashCode, msgStoreTime]
     * 2.RocksDBConsumeQueueOffsetTable uses to store physicalOffset and consumeQueueOffset(@see PhyAndCQOffset) of topic-queueId
     */
    private final ConsumeQueueRocksDBStorage rocksDBStorage;
    private final RocksDBConsumeQueueTable rocksDBConsumeQueueTable;
    private final RocksDBConsumeQueueOffsetTable rocksDBConsumeQueueOffsetTable;

    private final List<Pair<ByteBuffer, ByteBuffer>> cqBBPairList;
    private final List<Pair<ByteBuffer, ByteBuffer>> offsetBBPairList;
    private final Map<ByteBuffer, Pair<ByteBuffer, DispatchEntry>> tempTopicQueueMaxOffsetMap;
    private volatile boolean isCQError = false;

    private int consumeQueueByteBufferCacheIndex;
    private int offsetBufferCacheIndex;

    private final OffsetInitializer offsetInitializer;

    private final RocksGroupCommitService groupCommitService;

    private final AtomicReference<ServiceState> serviceState = new AtomicReference<>(ServiceState.CREATE_JUST);

    private final RocksDBCleanConsumeQueueService cleanConsumeQueueService;

    private long dispatchFromPhyOffset;

    /**
     * there are two threads to notify longPolling when build cq successfully
     *
     * @see DefaultMessageStore.ReputMessageService#doReput()
     * @see RocksGroupCommitService#groupCommit()
     * <p>
     * RocksDB CQ is build by RocksGroupCommitService, so we do not need to notify longPolling in
     * ReputMessageService
     */
    public RocksDBConsumeQueueStore(DefaultMessageStore messageStore) {
        super(messageStore);
        messageStore.setNotifyMessageArriveInBatch(true);

        this.storePath = StorePathConfigHelper.getStorePathConsumeQueue(messageStoreConfig.getStorePathRootDir());
        this.rocksDBStorage = new ConsumeQueueRocksDBStorage(messageStore, storePath);
        this.rocksDBConsumeQueueTable = new RocksDBConsumeQueueTable(rocksDBStorage, messageStore);
        this.rocksDBConsumeQueueOffsetTable = new RocksDBConsumeQueueOffsetTable(rocksDBConsumeQueueTable, rocksDBStorage, messageStore);

        this.offsetInitializer = new OffsetInitializerRocksDBImpl(this);
        this.groupCommitService = new RocksGroupCommitService(this);
        this.cqBBPairList = new ArrayList<>(16);
        this.offsetBBPairList = new ArrayList<>(DEFAULT_BYTE_BUFFER_CAPACITY);
        for (int i = 0; i < DEFAULT_BYTE_BUFFER_CAPACITY; i++) {
            this.cqBBPairList.add(RocksDBConsumeQueueTable.getCQByteBufferPair());
            this.offsetBBPairList.add(RocksDBConsumeQueueOffsetTable.getOffsetByteBufferPair());
        }

        this.tempTopicQueueMaxOffsetMap = new HashMap<>();
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("RocksDBConsumeQueueStoreScheduledThread", messageStore.getBrokerIdentity()));
        this.cleanConsumeQueueService = new RocksDBCleanConsumeQueueService();
    }

    private Pair<ByteBuffer, ByteBuffer> getCQByteBufferPair() {
        int idx = consumeQueueByteBufferCacheIndex++;
        if (idx >= cqBBPairList.size()) {
            this.cqBBPairList.add(RocksDBConsumeQueueTable.getCQByteBufferPair());
        }
        return cqBBPairList.get(idx);
    }

    private Pair<ByteBuffer, ByteBuffer> getOffsetByteBufferPair() {
        int idx = offsetBufferCacheIndex++;
        if (idx >= offsetBBPairList.size()) {
            this.offsetBBPairList.add(RocksDBConsumeQueueOffsetTable.getOffsetByteBufferPair());
        }
        return offsetBBPairList.get(idx);
    }

    @Override
    public void start() {
        if (serviceState.compareAndSet(ServiceState.CREATE_JUST, ServiceState.RUNNING)) {
            log.info("RocksDB ConsumeQueueStore start!");
            this.groupCommitService.start();
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                this.rocksDBStorage.statRocksdb(ROCKSDB_LOG);
            }, 10, this.messageStoreConfig.getStatRocksDBCQIntervalSec(), TimeUnit.SECONDS);

            this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
                cleanDirty(messageStore.getTopicConfigs().keySet());
            }, 10, this.messageStoreConfig.getCleanRocksDBDirtyCQIntervalMin(), TimeUnit.MINUTES);

            messageStore.getScheduledCleanQueueExecutorService().scheduleAtFixedRate(this.cleanConsumeQueueService::run,
                1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);
        }
    }

    private void cleanDirty(final Set<String> existTopicSet) {
        try {
            Map<String, Set<Integer>> topicQueueIdToBeDeletedMap =
                this.rocksDBConsumeQueueOffsetTable.iterateOffsetTable2FindDirty(existTopicSet);

            for (Map.Entry<String, Set<Integer>> entry : topicQueueIdToBeDeletedMap.entrySet()) {
                String topic = entry.getKey();
                for (int queueId : entry.getValue()) {
                    destroy(new RocksDBConsumeQueue(topic, queueId));
                }
            }
        } catch (Exception e) {
            log.error("cleanUnusedTopic Failed.", e);
        }
    }

    @Override
    public boolean load() {
        boolean result = this.rocksDBStorage.start();
        this.rocksDBConsumeQueueTable.load();
        this.rocksDBConsumeQueueOffsetTable.load();
        log.info("load rocksdb consume queue {}.", result ? "OK" : "Failed");
        return result;
    }
    @Override
    public void recover(boolean concurrently) throws RocksDBException {
        start();
        this.dispatchFromPhyOffset = getMaxPhyOffsetInConsumeQueue();
    }

    @Override
    public long getDispatchFromPhyOffset() {
        return dispatchFromPhyOffset;
    }

    @Override
    public boolean shutdown() {
        if (serviceState.compareAndSet(ServiceState.RUNNING, ServiceState.SHUTDOWN_ALREADY)) {
            if (this.groupCommitService != null) {
                this.groupCommitService.shutdown();
            }

            if (this.scheduledExecutorService != null) {
                this.scheduledExecutorService.shutdown();
            }
            return shutdownInner();
        }
        return true;
    }

    private boolean shutdownInner() {
        if (this.rocksDBStorage != null) {
            return this.rocksDBStorage.shutdown();
        }
        return true;
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) throws RocksDBException {
        if (null == request) {
            return;
        }

        try {
            groupCommitService.putRequest(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void putMessagePosition(List<DispatchRequest> requests) throws RocksDBException {
        final int maxRetries = 30;
        for (int i = 0; i < maxRetries; i++) {
            if (putMessagePosition0(requests)) {
                if (this.isCQError) {
                    this.messageStore.getRunningFlags().clearLogicsQueueError();
                    this.isCQError = false;
                }
                return;
            } else {
                ERROR_LOG.warn("Put cq Failed. retryTime: {}", i);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
        }
        if (!this.isCQError) {
            ERROR_LOG.error("[BUG] put CQ Failed.");
            this.messageStore.getRunningFlags().makeLogicsQueueError();
            this.isCQError = true;
        }
        throw new RocksDBException("put CQ Failed");
    }

    private boolean putMessagePosition0(List<DispatchRequest> requests) {
        if (!this.rocksDBStorage.hold()) {
            return false;
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            final int size = requests.size();
            if (size == 0) {
                return true;
            }
            long maxPhyOffset = 0;
            for (int i = size - 1; i >= 0; i--) {
                final DispatchRequest request = requests.get(i);
                DispatchEntry entry = DispatchEntry.from(request);
                dispatch(entry, writeBatch);
                dispatchLMQ(request, writeBatch);

                final int msgSize = request.getMsgSize();
                final long phyOffset = request.getCommitLogOffset();
                if (phyOffset + msgSize >= maxPhyOffset) {
                    maxPhyOffset = phyOffset + msgSize;
                }
            }

            this.rocksDBConsumeQueueOffsetTable.putMaxPhyAndCqOffset(tempTopicQueueMaxOffsetMap, writeBatch, maxPhyOffset);

            this.rocksDBStorage.batchPut(writeBatch);

            this.rocksDBConsumeQueueOffsetTable.putHeapMaxCqOffset(tempTopicQueueMaxOffsetMap);
            notifyMessageArriveAndClear(requests);
            return true;
        } catch (Exception e) {
            ERROR_LOG.error("putMessagePosition0 failed.", e);
            return false;
        } finally {
            tempTopicQueueMaxOffsetMap.clear();
            consumeQueueByteBufferCacheIndex = 0;
            offsetBufferCacheIndex = 0;
            this.rocksDBStorage.release();
        }
    }

    private void dispatch(@Nonnull DispatchEntry entry, @Nonnull final WriteBatch writeBatch) throws RocksDBException {
        this.rocksDBConsumeQueueTable.buildAndPutCQByteBuffer(getCQByteBufferPair(), entry, writeBatch);
        updateTempTopicQueueMaxOffset(getOffsetByteBufferPair(), entry);
    }

    private void updateTempTopicQueueMaxOffset(final Pair<ByteBuffer, ByteBuffer> offsetBBPair,
        final DispatchEntry entry) {
        RocksDBConsumeQueueOffsetTable.buildOffsetKeyAndValueByteBuffer(offsetBBPair, entry);
        ByteBuffer topicQueueId = offsetBBPair.getObject1();
        ByteBuffer maxOffsetBB = offsetBBPair.getObject2();
        Pair<ByteBuffer, DispatchEntry> old = tempTopicQueueMaxOffsetMap.get(topicQueueId);
        if (old == null) {
            tempTopicQueueMaxOffsetMap.put(topicQueueId, new Pair<>(maxOffsetBB, entry));
        } else {
            long oldMaxOffset = old.getObject1().getLong(RocksDBConsumeQueueOffsetTable.OFFSET_CQ_OFFSET);
            long maxOffset = maxOffsetBB.getLong(RocksDBConsumeQueueOffsetTable.OFFSET_CQ_OFFSET);
            if (maxOffset >= oldMaxOffset) {
                ERROR_LOG.error("cqOffset invalid1. old: {}, now: {}", oldMaxOffset, maxOffset);
            }
        }
    }

    private void dispatchLMQ(@Nonnull DispatchRequest request, @Nonnull final WriteBatch writeBatch)
        throws RocksDBException {
        if (!messageStoreConfig.isEnableLmq() || !request.containsLMQ()) {
            return;
        }
        Map<String, String> map = request.getPropertiesMap();
        String lmqNames = map.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String lmqOffsets = map.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        String[] queues = lmqNames.split(MixAll.LMQ_DISPATCH_SEPARATOR);
        String[] queueOffsets = lmqOffsets.split(MixAll.LMQ_DISPATCH_SEPARATOR);
        if (queues.length != queueOffsets.length) {
            ERROR_LOG.error("[bug] queues.length!=queueOffsets.length ", request.getTopic());
            return;
        }
        for (int i = 0; i < queues.length; i++) {
            String queueName = queues[i];
            DispatchEntry entry = DispatchEntry.from(request);
            long queueOffset = Long.parseLong(queueOffsets[i]);
            int queueId = request.getQueueId();
            if (this.messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                queueId = MixAll.LMQ_QUEUE_ID;
            }
            entry.queueId = queueId;
            entry.queueOffset = queueOffset;
            entry.topic = queueName.getBytes(StandardCharsets.UTF_8);
            log.debug("Dispatch LMQ[{}:{}]:{} --> {}", queueName, queueId, queueOffset, entry.commitLogOffset);
            dispatch(entry, writeBatch);
        }
    }

    private void notifyMessageArriveAndClear(List<DispatchRequest> requests) {
        try {
            for (DispatchRequest dp : requests) {
                this.messageStore.notifyMessageArriveIfNecessary(dp);
            }
            requests.clear();
        } catch (Exception e) {
            ERROR_LOG.error("notifyMessageArriveAndClear Failed.", e);
        }
    }

    public Statistics getStatistics() {
        return rocksDBStorage.getStatistics();
    }

    public List<ByteBuffer> rangeQuery(final String topic, final int queueId, final long startIndex,
        final int num) throws RocksDBException {
        return this.rocksDBConsumeQueueTable.rangeQuery(topic, queueId, startIndex, num);
    }

    public ByteBuffer get(final String topic, final int queueId, final long cqOffset) throws RocksDBException {
        return this.rocksDBConsumeQueueTable.getCQInKV(topic, queueId, cqOffset);
    }

    /**
     * Try to set topicQueueTable = new HashMap<>(), otherwise it will cause bug when broker role changes.
     * And unlike method in DefaultMessageStore, we don't need to really recover topic queue table advance,
     * because we can recover topic queue table from rocksdb when we need to use it.
     * @see RocksDBConsumeQueue#assignQueueOffset
     *
     * @see RocksDBConsumeQueue#increaseQueueOffset(QueueOffsetOperator, MessageExtBrokerInner, short)
     * @see org.apache.rocketmq.store.queue.RocksDBConsumeQueueOffsetTable#getMinCqOffset(String, int)
     */
    @Override
    public void recoverOffsetTable(long minPhyOffset) {
        this.setTopicQueueTable(new ConcurrentHashMap<>());
    }

    @Override
    public void destroy(boolean loadAfterDestroy) {
        try {
            shutdownInner();
            FileUtils.deleteDirectory(new File(this.storePath));
        } catch (Exception e) {
            ERROR_LOG.error("destroy cq Failed. {}", this.storePath, e);
        }
        if (loadAfterDestroy) {
            load();
        }
    }

    @Override
    public void destroy(ConsumeQueueInterface consumeQueue) throws RocksDBException {
        String topic = consumeQueue.getTopic();
        int queueId = consumeQueue.getQueueId();
        if (StringUtils.isEmpty(topic) || queueId < 0 || !this.rocksDBStorage.hold()) {
            return;
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            this.rocksDBConsumeQueueTable.destroyCQ(topic, queueId, writeBatch);
            this.rocksDBConsumeQueueOffsetTable.destroyOffset(topic, queueId, writeBatch);

            this.rocksDBStorage.batchPut(writeBatch);
        } catch (RocksDBException e) {
            ERROR_LOG.error("kv deleteTopic {} Failed.", topic, e);
            throw e;
        } finally {
            this.rocksDBStorage.release();
        }
    }

    /**
     * ConsumerQueueTable, as an in-memory data structure, uses lazy loading mechanism in RocksDBConsumeQueueStore.
     * This means that when the broker restarts, it may not be able to retrieve all ConsumerQueues from the table.
     * Therefore, before deleting a topic, we need to attempt to build all ConsumerQueues under that topic to ensure
     * the completeness of the deletion operation.
     */
    @Override
    public boolean deleteTopic(String topic) {
        try {
            Set<Integer> queueIds = rocksDBConsumeQueueOffsetTable.scanAllQueueIdInTopic(topic);
            queueIds.forEach(queueId -> findOrCreateConsumeQueue(topic, queueId));
        } catch (RocksDBException e) {
            ERROR_LOG.error("Failed to scan queueIds for topic. topic={}", topic, e);
        }
        return super.deleteTopic(topic);
    }

    @Override
    public void flush() throws StoreException {
        try (FlushOptions flushOptions = new FlushOptions()) {
            flushOptions.setWaitForFlush(true);
            flushOptions.setAllowWriteStall(true);
            this.rocksDBStorage.flush(flushOptions);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public void checkSelf() {
        // ignored
    }

    /**
     * We do not need to truncate dirty CQ in RocksDBConsumeQueueTable,  Because dirty CQ in RocksDBConsumeQueueTable
     * will be rewritten by new KV when new messages are appended or will be cleaned up when topics are deleted.
     * But dirty offset info in RocksDBConsumeQueueOffsetTable must be truncated, because we use offset info in
     * RocksDBConsumeQueueOffsetTable to rebuild topicQueueTable(@see RocksDBConsumeQueue#increaseQueueOffset).
     *
     * @param offsetToTruncate CommitLog offset to truncate to
     * @throws RocksDBException If there is any error.
     */
    @Override
    public void truncateDirty(long offsetToTruncate) throws RocksDBException {
        long maxPhyOffsetInRocksdb = getMaxPhyOffsetInConsumeQueue();
        if (offsetToTruncate >= maxPhyOffsetInRocksdb) {
            return;
        }

        this.rocksDBConsumeQueueOffsetTable.truncateDirty(offsetToTruncate);
    }

    @Override
    public void cleanExpired(final long minPhyOffset) {
        this.rocksDBStorage.manualCompaction(minPhyOffset);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp,
        BoundaryType boundaryType) throws RocksDBException {
        final long minPhysicOffset = this.messageStore.getMinPhyOffset();
        long low = this.rocksDBConsumeQueueOffsetTable.getMinCqOffset(topic, queueId);
        Long high = this.rocksDBConsumeQueueOffsetTable.getMaxCqOffset(topic, queueId);
        if (high == null || high == -1) {
            return 0;
        }
        return this.rocksDBConsumeQueueTable.binarySearchInCQByTime(topic, queueId, high, low, timestamp,
            minPhysicOffset, boundaryType);
    }

    /**
     * This method actually returns NEXT slot index to use, starting from 0. For example, if the queue is empty,
     * it returns 0, pointing to the first slot of the 0-based queue;
     *
     * @param topic   Topic name
     * @param queueId Queue ID
     * @return Index of the next slot to push into
     * @throws RocksDBException if RocksDB fails to fulfill the request.
     */
    public long getMaxOffsetInQueue(String topic, int queueId) throws RocksDBException {
        Long maxOffset = this.rocksDBConsumeQueueOffsetTable.getMaxCqOffset(topic, queueId);
        return (maxOffset != null) ? maxOffset + 1 : 0;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) throws RocksDBException {
        return this.rocksDBConsumeQueueOffsetTable.getMinCqOffset(topic, queueId);
    }

    public Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId) {
        return this.rocksDBConsumeQueueOffsetTable.getMaxPhyOffset(topic, queueId);
    }

    @Override
    public long getMaxPhyOffsetInConsumeQueue() throws RocksDBException {
        return this.rocksDBConsumeQueueOffsetTable.getMaxPhyOffset();
    }

    @Override
    public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueueInterface> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueueInterface> newMap;
            if (MixAll.isLmq(topic)) {
                // For LMQ, no need to over allocate internal hashtable
                newMap = new ConcurrentHashMap<>(1, 1.0F);
            } else {
                newMap = new ConcurrentHashMap<>(8);
            }
            ConcurrentMap<Integer, ConsumeQueueInterface> oldMap = this.consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueueInterface logic = map.get(queueId);
        if (logic != null) {
            return logic;
        }

        ConsumeQueueInterface newLogic = new RocksDBConsumeQueue(this.messageStore.getMessageStoreConfig(), this, topic, queueId);
        ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);

        return oldLogic != null ? oldLogic : newLogic;
    }

    @Override
    public ConsumeQueueInterface getConsumeQueue(String topic, int queueId) {
        return findOrCreateConsumeQueue(topic, queueId);
    }

    @Override
    public long getTotalSize() {
        return 0;
    }

    @Override
    public boolean isMappedFileMatchedRecover(long phyOffset, long storeTimestamp,
        boolean recoverNormally) {
        return phyOffset <= dispatchFromPhyOffset;
    }

    @Override
    public long getLmqQueueOffset(String topic, int queueId) throws ConsumeQueueException {
        return queueOffsetOperator.getLmqOffset(topic, queueId, offsetInitializer);
    }

    @Override
    public Long getMaxOffset(String topic, int queueId) throws ConsumeQueueException {
        if (MixAll.isLmq(topic)) {
            return getLmqQueueOffset(topic, queueId);
        }
        return super.getMaxOffset(topic, queueId);
    }

    public boolean isStopped() {
        return ServiceState.SHUTDOWN_ALREADY == serviceState.get();
    }

    public void updateCqOffset(final String topic, final int queueId, final long phyOffset,
        final long cqOffset, boolean max) throws RocksDBException {
        this.rocksDBConsumeQueueOffsetTable.updateCqOffset(topic, queueId, phyOffset, cqOffset, max);
    }

    class RocksDBCleanConsumeQueueService {
        protected long lastPhysicalMinOffset = 0;

        private final double diskSpaceWarningLevelRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        private final double diskSpaceCleanForciblyRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        public String getServiceName() {
            return messageStore.getBrokerConfig().getIdentifier() + ConsumeQueueStore.CleanConsumeQueueService.class.getSimpleName();
        }

        protected void deleteExpiredFiles() {
            long minOffset = messageStore.getCommitLog().getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                boolean spaceFull = isSpaceToDelete();
                boolean timeUp = messageStore.isTimeToDelete();
                if (spaceFull || timeUp) {
                    // To delete the CQ Units whose physical offset is smaller min physical offset in commitLog.
                    cleanExpired(minOffset);
                }

                messageStore.getIndexService().deleteExpiredFile(minOffset);
            }
        }

        private boolean isSpaceToDelete() {
            double ratio = messageStoreConfig.getDiskMaxUsedSpaceRatio() / 100.0;

            String storePathLogics = StorePathConfigHelper
                .getStorePathConsumeQueue(messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            if (logicsRatio > diskSpaceWarningLevelRatio) {
                boolean diskMaybeFull = messageStore.getRunningFlags().getAndMakeLogicDiskFull();
                if (diskMaybeFull) {
                    log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                }
            } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
            } else {
                boolean diskOk = messageStore.getRunningFlags().getAndMakeLogicDiskOK();
                if (!diskOk) {
                    log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                }
            }

            if (logicsRatio < 0 || logicsRatio > ratio) {
                log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                return true;
            }

            return false;
        }
    }
}
