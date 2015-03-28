/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.store;

import static com.alibaba.rocketmq.store.config.BrokerRole.SLAVE;

import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.SystemClock;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.running.RunningStats;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.store.config.StorePathConfigHelper;
import com.alibaba.rocketmq.store.ha.HAService;
import com.alibaba.rocketmq.store.index.IndexService;
import com.alibaba.rocketmq.store.index.QueryOffsetResult;
import com.alibaba.rocketmq.store.schedule.ScheduleMessageService;
import com.alibaba.rocketmq.store.stats.BrokerStatsManager;


/**
 * 存储层默认实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class DefaultMessageStore implements MessageStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    // 消息过滤
    private final MessageFilter messageFilter = new DefaultMessageFilter();
    // 存储配置
    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    private final CommitLog commitLog;
    // ConsumeQueue集合
    private final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;
    // 逻辑队列刷盘服务
    private final FlushConsumeQueueService flushConsumeQueueService;
    // 清理物理文件服务
    private final CleanCommitLogService cleanCommitLogService;
    // 清理逻辑文件服务
    private final CleanConsumeQueueService cleanConsumeQueueService;
    // 分发消息索引服务
    private final DispatchMessageService dispatchMessageService;
    // 消息索引服务
    private final IndexService indexService;
    // 预分配MapedFile对象服务
    private final AllocateMapedFileService allocateMapedFileService;
    // 从物理队列解析消息重新发送到逻辑队列
    private final ReputMessageService reputMessageService;
    // HA服务
    private final HAService haService;
    // 定时服务
    private final ScheduleMessageService scheduleMessageService;
    // 运行时数据统计
    private final StoreStatsService storeStatsService;
    // 运行过程标志位
    private final RunningFlags runningFlags = new RunningFlags();
    // 优化获取时间性能，精度1ms
    private final SystemClock systemClock = new SystemClock(1);
    // 存储服务是否启动
    private volatile boolean shutdown = true;
    // 存储检查点
    private StoreCheckpoint storeCheckpoint;
    // 权限控制后，打印间隔次数
    private AtomicLong printTimes = new AtomicLong(0);
    // 存储层的定时线程
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    private final BrokerStatsManager brokerStatsManager;


    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig,
            final BrokerStatsManager brokerStatsManager) throws IOException {
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMapedFileService = new AllocateMapedFileService();
        this.commitLog = new CommitLog(this);
        this.consumeQueueTable =
                new ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>>(
                    32);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.dispatchMessageService =
                new DispatchMessageService(this.messageStoreConfig.getPutMsgIndexHightWater());
        this.storeStatsService = new StoreStatsService();
        this.indexService = new IndexService(this);
        this.haService = new HAService(this);

        switch (this.messageStoreConfig.getBrokerRole()) {
        case SLAVE:
            this.reputMessageService = new ReputMessageService();
            // reputMessageService依赖scheduleMessageService做定时消息的恢复，确保储备数据一致
            this.scheduleMessageService = new ScheduleMessageService(this);
            break;
        case ASYNC_MASTER:
        case SYNC_MASTER:
            this.reputMessageService = null;
            this.scheduleMessageService = new ScheduleMessageService(this);
            break;
        default:
            this.reputMessageService = null;
            this.scheduleMessageService = null;
        }

        // load过程依赖此服务，所以提前启动
        this.allocateMapedFileService.start();
        this.dispatchMessageService.start();
        // 因为下面的recover会分发请求到索引服务，如果不启动，分发过程会被流控
        this.indexService.start();
    }


    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> tables =
                DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }


    /**
     * 加载数据
     * 
     * @throws IOException
     */
    public boolean load() {
        boolean result = true;

        try {
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", (lastExitOK ? "normally" : "abnormally"));

            // load 定时进度
            // 这个步骤要放置到最前面，从CommitLog里Recover定时消息需要依赖加载的定时级别参数
            // slave依赖scheduleMessageService做定时消息的恢复
            if (null != scheduleMessageService) {
                result = result && this.scheduleMessageService.load();
            }

            // load Commit Log
            result = result && this.commitLog.load();

            // load Consume Queue
            result = result && this.loadConsumeQueue();

            if (result) {
                this.storeCheckpoint =
                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig
                            .getStorePathRootDir()));

                this.indexService.load(lastExitOK);

                // 尝试恢复数据
                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        }
        catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMapedFileService.shutdown();
        }

        return result;
    }


    private void addScheduleTask() {
        // 定时删除过期文件
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        // 定时清理完全不使用的队列
        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
    }


    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
        this.cleanConsumeQueueService.run();
    }


    public void cleanExpiredConsumerQueue() {
        // CommitLog的最小Offset
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>>> it =
                this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentHashMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    // maxCLOffsetInConsumeQueue==-1有可能正好是索引文件刚好创建的那一时刻,此时不清除数据
                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn(
                            "maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",//
                            nextQT.getValue().getTopic(),//
                            nextQT.getValue().getQueueId(),//
                            nextQT.getValue().getMaxPhysicOffset(),//
                            nextQT.getValue().getMinLogicOffset());
                    }
                    else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                            "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",//
                            topic,//
                            nextQT.getKey(),//
                            minCommitLogOffset,//
                            maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueurFromTopicQueueTable(nextQT.getValue()
                            .getTopic(), nextQT.getValue().getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }


    /**
     * 启动存储服务
     * 
     * @throws Exception
     */
    public void start() throws Exception {
        // 在构造函数已经start了。
        // this.indexService.start();
        // 在构造函数已经start了。
        // this.dispatchMessageService.start();
        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        // slave不启动scheduleMessageService避免对消费队列的并发操作
        if (this.scheduleMessageService != null && SLAVE != messageStoreConfig.getBrokerRole()) {
            this.scheduleMessageService.start();
        }

        if (this.reputMessageService != null) {
            this.reputMessageService.setReputFromOffset(this.commitLog.getMaxOffset());
            this.reputMessageService.start();
        }

        this.haService.start();

        this.createTempFile();
        this.addScheduleTask();
        this.shutdown = false;
    }


    /**
     * 关闭存储服务
     */
    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();

            try {
                // 等待其他调用停止
                Thread.sleep(1000 * 3);
            }
            catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }

            this.haService.shutdown();

            this.storeStatsService.shutdown();
            this.dispatchMessageService.shutdown();
            this.indexService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.commitLog.shutdown();
            this.allocateMapedFileService.shutdown();
            if (this.reputMessageService != null) {
                this.reputMessageService.shutdown();
            }
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        }
    }


    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig
            .getStorePathRootDir()));
    }


    public void destroyLogics() {
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }


    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden "
                        + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
        else {
            this.printTimes.set(0);
        }

        // message topic长度校验
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // message properties长度校验
        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessage(msg);
        // 性能数据统计
        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 1000) {
            log.warn("putMessage not in lock eclipse time(ms) " + eclipseTime);
        }
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);
        this.storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();

        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }


    public SystemClock getSystemClock() {
        return systemClock;
    }


    public GetMessageResult getMessage(final String group, final String topic, final int queueId,
            final long offset, final int maxMsgNums, final SubscriptionData subscriptionData) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden "
                    + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now();

        // 枚举变量，取消息结果
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        // 当被过滤后，返回下一次开始的Offset
        long nextBeginOffset = offset;
        // 逻辑队列中的最小Offset
        long minOffset = 0;
        // 逻辑队列中的最大Offset
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        // 有个读写锁，所以只访问一次，避免锁开销影响性能
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = consumeQueue.getMinOffsetInQuque();
            maxOffset = consumeQueue.getMaxOffsetInQuque();

            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = 0;
            }
            else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = minOffset;
            }
            else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = offset;
            }
            else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = minOffset;
                }
                else {
                    nextBeginOffset = maxOffset;
                }
            }
            else {
                SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) {
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        int i = 0;
                        final int MaxFilterMessageCount = 16000;
                        boolean diskFallRecorded = false;
                        for (; i < bufferConsumeQueue.getSize() && i < MaxFilterMessageCount; i +=
                                ConsumeQueue.CQStoreUnitSize) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            maxPhyOffsetPulling = offsetPy;

                            // 说明物理文件正在被删除
                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            // 判断是否拉磁盘数据
                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                            // 此批消息达到上限了
                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(),
                                getResult.getMessageCount(), isInDisk)) {
                                break;
                            }

                            // 消息过滤
                            if (this.messageFilter.isMessageMatched(subscriptionData, tagsCode)) {
                                SelectMapedBufferResult selectResult =
                                        this.commitLog.getMessage(offsetPy, sizePy);
                                if (selectResult != null) {
                                    this.storeStatsService.getGetMessageTransferedMsgCount()
                                        .incrementAndGet();
                                    getResult.addMessage(selectResult);
                                    status = GetMessageStatus.FOUND;
                                    nextPhyFileStartOffset = Long.MIN_VALUE;

                                    // 统计读取磁盘落后情况
                                    if (diskFallRecorded) {
                                        diskFallRecorded = true;
                                        long fallBehind = consumeQueue.getMaxPhysicOffset() - offsetPy;
                                        brokerStatsManager.recordDiskFallBehind(group, topic, queueId,
                                            fallBehind);
                                    }
                                }
                                else {
                                    if (getResult.getBufferTotalSize() == 0) {
                                        status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                    }

                                    // 物理文件正在被删除，尝试跳过
                                    nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                }
                            }
                            else {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                if (log.isDebugEnabled()) {
                                    log.debug("message type not matched, client: " + subscriptionData
                                            + " server: " + tagsCode);
                                }
                            }
                        }

                        nextBeginOffset = offset + (i / ConsumeQueue.CQStoreUnitSize);

                        // TODO 是否会影响性能，需要测试
                        long diff = this.getMaxPhyOffset() - maxPhyOffsetPulling;
                        long memory =
                                (long) (StoreUtil.TotalPhysicalMemorySize * (this.messageStoreConfig
                                    .getAccessMessageInMemoryMaxRatio() / 100.0));
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    }
                    finally {
                        // 必须释放资源
                        bufferConsumeQueue.release();
                    }
                }
                else {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = consumeQueue.rollNextFile(offset);
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: "
                            + minOffset + " maxOffset: " + maxOffset + ", but access logic queue failed.");
                }
            }
        }
        // 请求的队列Id没有
        else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = 0;
        }

        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        }
        else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long eclipseTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(eclipseTime);

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }


    /**
     * 返回的是当前队列的最大Offset，这个Offset没有对应的消息
     */
    public long getMaxOffsetInQuque(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQuque();
            return offset;
        }

        return 0;
    }


    /**
     * 返回的是当前队列的最小Offset
     */
    public long getMinOffsetInQuque(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQuque();
        }

        return -1;
    }


    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }


    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMapedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            }
            finally {
                sbr.release();
            }
        }

        return null;
    }


    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMapedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            }
            finally {
                sbr.release();
            }
        }

        return null;
    }


    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }


    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }


    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();
        // 检测物理文件磁盘空间
        {
            String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        // 检测逻辑文件磁盘空间
        {

            String storePathLogics =
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig
                        .getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        // 延时进度
        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(),
            String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(),
            String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }


    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }


    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMapedBufferResult result =
                    logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQStoreUnitSize);
            if (result != null) {
                try {
                    final long phyOffset = result.getByteBuffer().getLong();
                    final int size = result.getByteBuffer().getInt();
                    long storeTime = this.getCommitLog().pickupStoretimestamp(phyOffset, size);
                    return storeTime;
                }
                catch (Exception e) {
                }
                finally {
                    result.release();
                }
            }
        }

        return -1;
    }


    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long offset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMapedBufferResult result = logicQueue.getIndexBuffer(offset);
            if (result != null) {
                try {
                    final long phyOffset = result.getByteBuffer().getLong();
                    final int size = result.getByteBuffer().getInt();
                    long storeTime = this.getCommitLog().pickupStoretimestamp(phyOffset, size);
                    return storeTime;
                }
                catch (Exception e) {
                }
                finally {
                    result.release();
                }
            }
        }

        return -1;
    }


    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }


    @Override
    public SelectMapedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }


    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data);
        if (result) {
            this.reputMessageService.wakeup();
        }
        else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }


    @Override
    public void excuteDeleteFilesManualy() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }


    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult =
                    this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            // 从小到达排序
            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {
                    // 在服务器检验Hash冲突
                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
                    if (topic.equals(msg.getTopic())) {
                        for (String k : keyArray) {
                            if (k.equals(key)) {
                                match = true;
                                break;
                            }
                        }
                    }

                    if (match) {
                        SelectMapedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    }
                    else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                }
                catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            // 只要查到记录就返回
            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            // 都遍历完了， 但是没有找到消息
            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }


    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }


    @Override
    public long now() {
        return this.systemClock.now();
    }


    public CommitLog getCommitLog() {
        return commitLog;
    }


    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMapedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            }
            finally {
                sbr.release();
            }
        }

        return null;
    }


    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentHashMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentHashMap<Integer, ConsumeQueue> newMap =
                    new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentHashMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            }
            else {
                map = newMap;
            }
        }

        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            ConsumeQueue newLogic =
                    new ConsumeQueue(//
                        topic,//
                        queueId,//
                        StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig
                            .getStorePathRootDir()),//
                        this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),//
                        this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            }
            else {
                logic = newLogic;
            }
        }

        return logic;
    }


    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal,
            boolean isInDisk) {
        // 第一条消息可以不做限制
        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if ((messageTotal + 1) >= maxMsgNums) {
            return true;
        }

        // 消息在磁盘
        if (isInDisk) {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            if ((messageTotal + 1) > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk()) {
                return true;
            }
        }
        // 消息在内存
        else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if ((messageTotal + 1) > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory()) {
                return true;
            }
        }

        return false;
    }


    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }


    /**
     * 启动服务后，在存储根目录创建临时文件，类似于 UNIX VI编辑工具
     * 
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MapedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }


    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }


    private boolean loadConsumeQueue() {
        File dirLogic =
                new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig
                    .getStorePathRootDir()));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {
            // TOPIC 遍历
            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();
                // TOPIC 下队列遍历
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId = Integer.parseInt(fileQueueId.getName());
                        ConsumeQueue logic =
                                new ConsumeQueue(//
                                    topic,//
                                    queueId,//
                                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig
                                        .getStorePathRootDir()),//
                                    this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),//
                                    this);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }


    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }


    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentHashMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        }
        else {
            map.put(queueId, consumeQueue);
        }
    }


    private void recover(final boolean lastExitOK) {
        // 先按照正常流程恢复Consume Queue
        this.recoverConsumeQueue();

        // 正常数据恢复
        if (lastExitOK) {
            this.commitLog.recoverNormally();
        }
        // 异常数据恢复，OS CRASH或者JVM CRASH或者机器掉电
        else {
            this.commitLog.recoverAbnormally();
        }

        // 保证消息都能从DispatchService缓冲队列进入到真正的队列
        while (this.dispatchMessageService.hasRemainMessage()) {
            try {
                Thread.sleep(500);
                log.info("waiting dispatching message over");
            }
            catch (InterruptedException e) {
            }
        }

        this.recoverTopicQueueTable();
    }


    private void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                // 恢复写入消息时，记录的队列offset
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQuque());
                // 恢复每个队列的最小offset
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }


    private void recoverConsumeQueue() {
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
            }
        }
    }


    public void putMessagePostionInfo(String topic, int queueId, long offset, int size, long tagsCode,
            long storeTimestamp, long logicOffset) {
        ConsumeQueue cq = this.findConsumeQueue(topic, queueId);
        cq.putMessagePostionInfoWrapper(offset, size, tagsCode, storeTimestamp, logicOffset);
    }


    public void putDispatchRequest(final DispatchRequest dispatchRequest) {
        this.dispatchMessageService.putRequest(dispatchRequest);
    }


    public DispatchMessageService getDispatchMessageService() {
        return dispatchMessageService;
    }


    public AllocateMapedFileService getAllocateMapedFileService() {
        return allocateMapedFileService;
    }


    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }


    public RunningFlags getAccessRights() {
        return runningFlags;
    }


    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }


    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }


    public HAService getHaService() {
        return haService;
    }


    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }


    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    /**
     * 清理物理文件服务
     */
    class CleanCommitLogService {
        // 手工触发一次最多删除次数
        private final static int MaxManualDeleteFileTimes = 20;
        // 磁盘空间警戒水位，超过，则停止接收新消息（出于保护自身目的）
        private final double DiskSpaceWarningLevelRatio = Double.parseDouble(System.getProperty(
            "rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));
        // 磁盘空间强制删除文件水位
        private final double DiskSpaceCleanForciblyRatio = Double.parseDouble(System.getProperty(
            "rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));
        private long lastRedeleteTimestamp = 0;
        // 手工触发删除消息
        private volatile int manualDeleteFileSeveralTimes = 0;
        // 立刻开始强制删除文件
        private volatile boolean cleanImmediately = false;


        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MaxManualDeleteFileTimes;
            DefaultMessageStore.log.info("excuteDeleteFilesManualy was invoked");
        }


        public void run() {
            try {
                this.deleteExpiredFiles();

                this.redeleteHangedFile();
            }
            catch (Exception e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }


        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }


        /**
         * 最前面的文件有可能Hang住，定期检查一下
         */
        private void redeleteHangedFile() {
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMapedFileIntervalForcibly =
                        DefaultMessageStore.this.getMessageStoreConfig()
                            .getDestroyMapedFileIntervalForcibly();
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                    // TODO
                }
            }
        }


        private void deleteExpiredFiles() {
            int deleteCount = 0;
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            int deletePhysicFilesInterval =
                    DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            int destroyMapedFileIntervalForcibly =
                    DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            boolean timeup = this.isTimeToDelete();
            boolean spacefull = this.isSpaceToDelete();
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            // 删除物理队列文件
            if (timeup || spacefull || manualDelete) {

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                // 是否立刻强制删除文件
                boolean cleanAtOnce =
                        DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable()
                                && this.cleanImmediately;

                log.info(
                    "begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",//
                    fileReservedTime,//
                    timeup,//
                    spacefull,//
                    manualDeleteFileSeveralTimes,//
                    cleanAtOnce);

                // 小时转化成毫秒
                fileReservedTime *= 60 * 60 * 1000;

                deleteCount =
                        DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime,
                            deletePhysicFilesInterval, destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                    // TODO
                }
                // 危险情况：磁盘满了，但是又无法删除文件
                else if (spacefull) {
                    // XXX: warn and notify me
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }


        /**
         * 是否可以删除文件，空间是否满足
         */
        private boolean isSpaceToDelete() {
            double ratio =
                    DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            cleanImmediately = false;

            // 检测物理文件磁盘空间
            {
                String storePathPhysic =
                        DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                if (physicRatio > DiskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio
                                + ", so mark disk full");
                        System.gc();
                    }

                    cleanImmediately = true;
                }
                else if (physicRatio > DiskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                }
                else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio
                                + ", so mark disk ok");
                    }
                }

                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, "
                            + physicRatio);
                    return true;
                }
            }

            // 检测逻辑文件磁盘空间
            {
                String storePathLogics =
                        StorePathConfigHelper.getStorePathConsumeQueue(DefaultMessageStore.this
                            .getMessageStoreConfig().getStorePathRootDir());
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > DiskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio
                                + ", so mark disk full");
                        System.gc();
                    }

                    cleanImmediately = true;
                }
                else if (logicsRatio > DiskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                }
                else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio
                                + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, "
                            + logicsRatio);
                    return true;
                }
            }

            return false;
        }


        /**
         * 是否可以删除文件，时间是否满足
         */
        private boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }


        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }


        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
    }

    /**
     * 清理逻辑文件服务
     */
    class CleanConsumeQueueService {
        private long lastPhysicalMinOffset = 0;


        private void deleteExpiredFiles() {
            int deleteLogicsFilesInterval =
                    DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                // 删除逻辑队列文件
                ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> tables =
                        DefaultMessageStore.this.consumeQueueTable;

                for (ConcurrentHashMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            }
                            catch (InterruptedException e) {
                            }
                        }
                    }
                }

                // 删除索引
                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }


        public void run() {
            try {
                this.deleteExpiredFiles();
            }
            catch (Exception e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }


        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    /**
     * 逻辑队列刷盘服务
     */
    class FlushConsumeQueueService extends ServiceThread {
        private static final int RetryTimesOver = 3;
        private long lastFlushTimestamp = 0;


        private void doFlush(int retryTimes) {
            /**
             * 变量含义：如果大于0，则标识这次刷盘必须刷多少个page，如果=0，则有多少刷多少
             */
            int flushConsumeQueueLeastPages =
                    DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RetryTimesOver) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            // 定时刷盘
            int flushConsumeQueueThoroughInterval =
                    DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> tables =
                    DefaultMessageStore.this.consumeQueueTable;

            for (ConcurrentHashMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = cq.commit(flushConsumeQueueLeastPages);
                    }
                }
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }


        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    int interval =
                            DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                }
                catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 正常shutdown时，要保证全部刷盘才退出
            this.doFlush(RetryTimesOver);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }

    /**
     * 分发消息索引服务
     */
    class DispatchMessageService extends ServiceThread {
        private volatile List<DispatchRequest> requestsWrite;
        private volatile List<DispatchRequest> requestsRead;


        public DispatchMessageService(int putMsgIndexHightWater) {
            putMsgIndexHightWater *= 1.5;
            this.requestsWrite = new ArrayList<DispatchRequest>(putMsgIndexHightWater);
            this.requestsRead = new ArrayList<DispatchRequest>(putMsgIndexHightWater);
        }


        public boolean hasRemainMessage() {
            List<DispatchRequest> reqs = this.requestsWrite;
            if (reqs != null && !reqs.isEmpty()) {
                return true;
            }

            reqs = this.requestsRead;
            if (reqs != null && !reqs.isEmpty()) {
                return true;
            }

            return false;
        }


        public void putRequest(final DispatchRequest dispatchRequest) {
            int requestsWriteSize = 0;
            int putMsgIndexHightWater =
                    DefaultMessageStore.this.getMessageStoreConfig().getPutMsgIndexHightWater();
            synchronized (this) {
                this.requestsWrite.add(dispatchRequest);
                requestsWriteSize = this.requestsWrite.size();
                if (!this.hasNotified) {
                    this.hasNotified = true;
                    this.notify();
                }
            }

            DefaultMessageStore.this.getStoreStatsService().setDispatchMaxBuffer(requestsWriteSize);

            // 这里主动做流控，防止CommitLog写入太快，导致消费队列被冲垮
            if (requestsWriteSize > putMsgIndexHightWater) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Message index buffer size " + requestsWriteSize + " > high water "
                                + putMsgIndexHightWater);
                    }

                    Thread.sleep(1);
                }
                catch (InterruptedException e) {
                }
            }
        }


        private void swapRequests() {
            List<DispatchRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }


        private void doDispatch() {
            if (!this.requestsRead.isEmpty()) {
                for (DispatchRequest req : this.requestsRead) {

                    final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());
                    // 1、分发消息位置信息到ConsumeQueue
                    switch (tranType) {
                    case MessageSysFlag.TransactionNotType:
                    case MessageSysFlag.TransactionCommitType:
                        // 将请求发到具体的Consume Queue
                        DefaultMessageStore.this.putMessagePostionInfo(req.getTopic(), req.getQueueId(),
                            req.getCommitLogOffset(), req.getMsgSize(), req.getTagsCode(),
                            req.getStoreTimestamp(), req.getConsumeQueueOffset());
                        break;
                    case MessageSysFlag.TransactionPreparedType:
                    case MessageSysFlag.TransactionRollbackType:
                        break;
                    }
                }

                if (DefaultMessageStore.this.getMessageStoreConfig().isMessageIndexEnable()) {
                    DefaultMessageStore.this.indexService.putRequest(this.requestsRead.toArray());
                }

                this.requestsRead.clear();
            }
        }


        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.waitForRunning(0);
                    this.doDispatch();
                }
                catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 在正常shutdown情况下，要保证所有消息都dispatch
            try {
                Thread.sleep(5 * 1000);
            }
            catch (InterruptedException e) {
                DefaultMessageStore.log.warn("DispatchMessageService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doDispatch();

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }


        @Override
        public String getServiceName() {
            return DispatchMessageService.class.getSimpleName();
        }
    }

    /**
     * SLAVE: 从物理队列Load消息，并分发到各个逻辑队列
     */
    class ReputMessageService extends ServiceThread {
        // 从这里开始解析物理队列数据，并分发到逻辑队列
        private volatile long reputFromOffset = 0;


        public long getReputFromOffset() {
            return reputFromOffset;
        }


        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }


        private void doReput() {
            for (boolean doNext = true; doNext;) {
                SelectMapedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        // 当主机有很多数据，备机没有数据时，此时启动备机，备机会从主机的末尾开始拉数据
                        // 这时reputFromOffset的初始值和commitlog的值不匹配。
                        this.reputFromOffset = result.getStartOffset();

                        for (int readSize = 0; readSize < result.getSize() && doNext;) {
                            DispatchRequest dispatchRequest =
                                    DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(
                                        result.getByteBuffer(), false, false);
                            int size = dispatchRequest.getMsgSize();
                            // 正常数据
                            if (size > 0) {
                                DefaultMessageStore.this.putDispatchRequest(dispatchRequest);

                                // FIXED BUG By shijia
                                this.reputFromOffset += size;
                                readSize += size;
                                DefaultMessageStore.this.storeStatsService
                                    .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic())
                                    .incrementAndGet();
                                DefaultMessageStore.this.storeStatsService.getSinglePutMessageTopicSizeTotal(
                                    dispatchRequest.getTopic()).addAndGet(dispatchRequest.getMsgSize());
                            }
                            // 文件中间读到错误
                            else if (size == -1) {
                                doNext = false;
                            }
                            // 走到文件末尾，切换至下一个文件
                            else if (size == 0) {
                                this.reputFromOffset =
                                        DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                readSize = result.getSize();
                            }
                        }
                    }
                    finally {
                        result.release();
                    }
                }
                else {
                    doNext = false;
                }
            }
        }


        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.waitForRunning(1000);
                    this.doReput();
                }
                catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }


    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long cqOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(cqOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                }
                finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }


    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }


    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }


    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>>> it =
                this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            // Topic可以删除
            if (!topics.contains(topic) && !topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentHashMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",//
                        cq.getTopic(), //
                        cq.getQueueId() //
                    );

                    this.commitLog.removeQueurFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }


    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset,
            long maxOffset, SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQuque());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQuque());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQStoreUnitSize) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
                            String msgId =
                                    MessageDecoder.createMessageId(msgIdMemory,
                                        MessageExt.SocketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    }
                    finally {
                        // 必须释放资源
                        bufferConsumeQueue.release();
                    }
                }
                else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }


    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory =
                (long) (StoreUtil.TotalPhysicalMemorySize * (this.messageStoreConfig
                    .getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }


    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {
        // 有个读写锁，所以只访问一次，避免锁开销影响性能
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize();) {
                        i += ConsumeQueue.CQStoreUnitSize;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                }
                finally {
                    // 必须释放资源
                    bufferConsumeQueue.release();
                }
            }
            else {
                return false;
            }
        }
        return false;
    }


    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }
}
