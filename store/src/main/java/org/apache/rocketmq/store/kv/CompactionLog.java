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
package org.apache.rocketmq.store.kv;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CompactionAppendMsgCallback;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageLock;
import org.apache.rocketmq.store.PutMessageReentrantLock;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.StoreUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.BatchConsumeQueue;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.queue.SparseConsumeQueue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.rocketmq.common.message.MessageDecoder.BLANK_MAGIC_CODE;

public class CompactionLog {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
    private static final int MAX_PULL_MSG_SIZE = 128 * 1024 * 1024;
    public static final String COMPACTING_SUB_FOLDER = "compacting";
    public static final String REPLICATING_SUB_FOLDER = "replicating";

    private final int compactionLogMappedFileSize;
    private final int compactionCqMappedFileSize;
    private final String compactionLogFilePath;
    private final String compactionCqFilePath;
    private final MessageStore defaultMessageStore;
    private final CompactionStore compactionStore;
    private final MessageStoreConfig messageStoreConfig;
    private final CompactionAppendMsgCallback endMsgCallback;
    private final String topic;
    private final int queueId;
    private final int offsetMapMemorySize;
    private final PutMessageLock putMessageLock;
    private final PutMessageLock readMessageLock;
    private TopicPartitionLog current;
    private TopicPartitionLog compacting;
    private TopicPartitionLog replicating;
    private final CompactionPositionMgr positionMgr;
    private final AtomicReference<State> state;

    public CompactionLog(final MessageStore messageStore, final CompactionStore compactionStore, final String topic, final int queueId)
        throws IOException {
        this.topic = topic;
        this.queueId = queueId;
        this.defaultMessageStore = messageStore;
        this.compactionStore = compactionStore;
        this.messageStoreConfig = messageStore.getMessageStoreConfig();
        this.offsetMapMemorySize = compactionStore.getOffsetMapSize();
        this.compactionCqMappedFileSize =
            messageStoreConfig.getCompactionCqMappedFileSize() / BatchConsumeQueue.CQ_STORE_UNIT_SIZE
                * BatchConsumeQueue.CQ_STORE_UNIT_SIZE;
        this.compactionLogMappedFileSize = getCompactionLogSize(compactionCqMappedFileSize,
            messageStoreConfig.getCompactionMappedFileSize());
        this.compactionLogFilePath = Paths.get(compactionStore.getCompactionLogPath(),
            topic, String.valueOf(queueId)).toString();
        this.compactionCqFilePath = compactionStore.getCompactionCqPath();        // batch consume queue already separated
        this.positionMgr = compactionStore.getPositionMgr();

        this.putMessageLock =
            messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() :
                new PutMessageSpinLock();
        this.readMessageLock =
            messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() :
                new PutMessageSpinLock();
        this.endMsgCallback = new CompactionAppendEndMsgCallback();
        this.state = new AtomicReference<>(State.INITIALIZING);
        log.info("CompactionLog {}:{} init completed.", topic, queueId);
    }

    private int getCompactionLogSize(int cqSize, int origLogSize) {
        int n = origLogSize / cqSize;
        if (n < 5) {
            return cqSize * 5;
        }
        int m = origLogSize % cqSize;
        if (m > 0 && m < (cqSize >> 1)) {
            return n * cqSize;
        } else {
            return (n + 1) * cqSize;
        }
    }

    public void load(boolean exitOk) throws IOException, RuntimeException {
        initLogAndCq(exitOk);
        if (defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE
            && getLog().isMappedFilesEmpty()) {
            log.info("{}:{} load compactionLog from remote master", topic, queueId);
            loadFromRemoteAsync();
        } else {
            state.compareAndSet(State.INITIALIZING, State.NORMAL);
        }
    }

    private void initLogAndCq(boolean exitOk) throws IOException, RuntimeException {
        current = new TopicPartitionLog(this);
        current.init(exitOk);
    }


    private boolean putMessageFromRemote(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        // split bytebuffer to avoid encode message again
        while (byteBuffer.hasRemaining()) {
            int mark = byteBuffer.position();
            ByteBuffer bb = byteBuffer.slice();
            int size = bb.getInt();
            if (size < 0 || size > byteBuffer.capacity()) {
                break;
            } else {
                bb.limit(size);
                bb.rewind();
            }

            MessageExt messageExt = MessageDecoder.decode(bb, false, false);
            long messageOffset = messageExt.getQueueOffset();
            long minOffsetInQueue = getCQ().getMinOffsetInQueue();
            if (getLog().isMappedFilesEmpty() || messageOffset < minOffsetInQueue) {
                asyncPutMessage(bb, messageExt, replicating);
            } else {
                log.info("{}:{} message offset {} >= minOffsetInQueue {}, stop pull...",
                    topic, queueId, messageOffset, minOffsetInQueue);
                return false;
            }

            byteBuffer.position(mark + size);
        }

        return true;

    }

    private void pullMessageFromMaster() throws Exception {

        if (StringUtils.isBlank(compactionStore.getMasterAddr())) {
            compactionStore.getCompactionSchedule().schedule(() -> {
                try {
                    pullMessageFromMaster();
                } catch (Exception e) {
                    log.error("pullMessageFromMaster exception: ", e);
                }
            }, 5, TimeUnit.SECONDS);
            return;
        }

        replicating = new TopicPartitionLog(this, REPLICATING_SUB_FOLDER);
        try (MessageFetcher messageFetcher = new MessageFetcher()) {
            messageFetcher.pullMessageFromMaster(topic, queueId, getCQ().getMinOffsetInQueue(),
                compactionStore.getMasterAddr(), (currOffset, response) -> {
                    if (currOffset < 0) {
                        log.info("{}:{} current offset {}, stop pull...", topic, queueId, currOffset);
                        return false;
                    }
                    return putMessageFromRemote(response.getBody());
//                    positionMgr.setOffset(topic, queueId, currOffset);
                });
        }

        // merge files
        if (getLog().isMappedFilesEmpty()) {
            replaceFiles(getLog().getMappedFiles(), current, replicating);
        } else if (replicating.getLog().isMappedFilesEmpty()) {
            log.info("replicating message is empty");   //break
        } else {
            List<MappedFile> newFiles = Lists.newArrayList();
            List<MappedFile> toCompactFiles = Lists.newArrayList(replicating.getLog().getMappedFiles());
            putMessageLock.lock();
            try {
                // combine current and replicating to mappedFileList
                newFiles = Lists.newArrayList(getLog().getMappedFiles());
                toCompactFiles.addAll(newFiles);  //all from current
                current.roll(toCompactFiles.size() * compactionLogMappedFileSize);
            } catch (Throwable e) {
                log.error("roll log and cq exception: ", e);
            } finally {
                putMessageLock.unlock();
            }

            try {
                // doCompaction with current and replicating
                compactAndReplace(new ProcessFileList(toCompactFiles, toCompactFiles));
            } catch (Throwable e) {
                log.error("do merge replicating and current exception: ", e);
            }
        }

        // cleanReplicatingResource, force clean cq
        replicating.clean(false, true);

//        positionMgr.setOffset(topic, queueId, currentPullOffset);
        state.compareAndSet(State.INITIALIZING, State.NORMAL);
    }
    private void loadFromRemoteAsync() {
        compactionStore.getCompactionSchedule().submit(() -> {
            try {
                pullMessageFromMaster();
            } catch (Exception e) {
                log.error("fetch message from master exception: ", e);
            }
        });

        // update (currentStatus) = LOADING

        // request => get (start, end)
        // pull message => current message offset > end
        // done
        // positionMgr.persist();

        // update (currentStatus) = RUNNING
    }

    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (messageStoreConfig.getBrokerRole() != BrokerRole.SLAVE || messageStoreConfig.isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE *
            (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }

    private boolean isTheBatchFull(int sizePy, int unitBatchNum, int maxMsgNums, long maxMsgSize,
        int bufferTotal, int messageTotal, boolean isInDisk) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if (messageTotal + unitBatchNum > maxMsgNums) {
            return true;
        }

        if (bufferTotal + sizePy > maxMsgSize) {
            return true;
        }

        if (isInDisk) {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }
        } else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    public long rollNextFile(final long offset) {
        return offset + compactionLogMappedFileSize - offset % compactionLogMappedFileSize;
    }

    boolean shouldRetainMsg(final MessageExt msgExt, final OffsetMap map) throws DigestException {
        if (msgExt.getQueueOffset() > map.getLastOffset()) {
            return true;
        }

        String key = msgExt.getKeys();
        if (StringUtils.isNotBlank(key)) {
            boolean keyNotExistOrOffsetBigger = msgExt.getQueueOffset() >= map.get(key);
            boolean hasBody = ArrayUtils.isNotEmpty(msgExt.getBody());
            return keyNotExistOrOffsetBigger && hasBody;
        } else {
            log.error("message has no keys");
            return false;
        }
    }

    public void checkAndPutMessage(final SelectMappedBufferResult selectMappedBufferResult, final MessageExt msgExt,
        final OffsetMap offsetMap, final TopicPartitionLog tpLog)
        throws DigestException {
        if (shouldRetainMsg(msgExt, offsetMap)) {
            asyncPutMessage(selectMappedBufferResult.getByteBuffer(), msgExt, tpLog);
        }
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(final SelectMappedBufferResult selectMappedBufferResult) {
        return asyncPutMessage(selectMappedBufferResult, current);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(final SelectMappedBufferResult selectMappedBufferResult,
        final TopicPartitionLog tpLog) {
        MessageExt msgExt = MessageDecoder.decode(selectMappedBufferResult.getByteBuffer(), false, false);
        return asyncPutMessage(selectMappedBufferResult.getByteBuffer(), msgExt, tpLog);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(final ByteBuffer msgBuffer,
        final MessageExt msgExt, final TopicPartitionLog tpLog) {
        return asyncPutMessage(msgBuffer, msgExt.getTopic(), msgExt.getQueueId(),
            msgExt.getQueueOffset(), msgExt.getMsgId(), msgExt.getKeys(),
            MessageExtBrokerInner.tagsString2tagsCode(msgExt.getTags()), msgExt.getStoreTimestamp(), tpLog);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(final ByteBuffer msgBuffer,
        final DispatchRequest dispatchRequest) {
        return asyncPutMessage(msgBuffer, dispatchRequest.getTopic(), dispatchRequest.getQueueId(),
            dispatchRequest.getConsumeQueueOffset(), dispatchRequest.getUniqKey(), dispatchRequest.getKeys(),
            dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(), current);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(final ByteBuffer msgBuffer,
        final DispatchRequest dispatchRequest, final TopicPartitionLog tpLog) {
        return asyncPutMessage(msgBuffer, dispatchRequest.getTopic(), dispatchRequest.getQueueId(),
            dispatchRequest.getConsumeQueueOffset(), dispatchRequest.getUniqKey(), dispatchRequest.getKeys(),
            dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(), tpLog);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(final ByteBuffer msgBuffer,
        String topic, int queueId, long queueOffset, String msgId, String keys, long tagsCode, long storeTimestamp, final TopicPartitionLog tpLog) {

        // fix duplicate
        if (tpLog.getCQ().getMaxOffsetInQueue() - 1 >= queueOffset) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        if (StringUtils.isBlank(keys)) {
            log.warn("message {}-{}:{} have no key, will not put in compaction log", topic, queueId, msgId);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        putMessageLock.lock();
        try {
            long beginTime = System.nanoTime();

            if (tpLog.isEmptyOrCurrentFileFull()) {
                try {
                    tpLog.roll();
                } catch (IOException e) {
                    log.error("create mapped file or consumerQueue exception: ", e);
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }
            }

            MappedFile mappedFile = tpLog.getLog().getLastMappedFile();

            CompactionAppendMsgCallback callback = new CompactionAppendMessageCallback(topic, queueId, tagsCode, storeTimestamp, tpLog.getCQ());
            AppendMessageResult result = mappedFile.appendMessage(msgBuffer, callback);

            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    try {
                        tpLog.roll();
                    } catch (IOException e) {
                        log.error("create mapped file2 error, topic: {}, msgId: {}", topic, msgId);
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                    }
                    mappedFile = tpLog.getLog().getLastMappedFile();
                    result = mappedFile.appendMessage(msgBuffer, callback);
                    break;
                default:
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_OK, result));
        } finally {
            putMessageLock.unlock();
        }
    }

    private SelectMappedBufferResult getMessage(final long offset, final int size) {

        MappedFile mappedFile = this.getLog().findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % compactionLogMappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    private boolean validateCqUnit(CqUnit cqUnit) {
        return cqUnit.getPos() >= 0
            && cqUnit.getSize() > 0
            && cqUnit.getQueueOffset() >= 0
            && cqUnit.getBatchNum() > 0;
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums, final int maxTotalMsgSize) {
        readMessageLock.lock();
        try {
            long beginTime = System.nanoTime();

            GetMessageStatus status;
            long nextBeginOffset = offset;
            long minOffset = 0;
            long maxOffset = 0;

            GetMessageResult getResult = new GetMessageResult();

            final long maxOffsetPy = getLog().getMaxOffset();

            SparseConsumeQueue consumeQueue = getCQ();
            if (consumeQueue != null) {
                minOffset = consumeQueue.getMinOffsetInQueue();
                maxOffset = consumeQueue.getMaxOffsetInQueue();

                if (maxOffset == 0) {
                    status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                    nextBeginOffset = nextOffsetCorrection(offset, 0);
                } else if (offset == maxOffset) {
                    status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                    nextBeginOffset = nextOffsetCorrection(offset, offset);
                } else if (offset > maxOffset) {
                    status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                    if (0 == minOffset) {
                        nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                    } else {
                        nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                    }
                } else {

                    long maxPullSize = Math.max(maxTotalMsgSize, 100);
                    if (maxPullSize > MAX_PULL_MSG_SIZE) {
                        log.warn("The max pull size is too large maxPullSize={} topic={} queueId={}",
                            maxPullSize, topic, queueId);
                        maxPullSize = MAX_PULL_MSG_SIZE;
                    }
                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                    long maxPhyOffsetPulling = 0;
                    int cqFileNum = 0;

                    while (getResult.getBufferTotalSize() <= 0 && nextBeginOffset < maxOffset
                        && cqFileNum++ < this.messageStoreConfig.getTravelCqFileNumWhenGetMessage()) {
                        ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFromOrNext(nextBeginOffset);

                        if (bufferConsumeQueue == null) {
                            status = GetMessageStatus.OFFSET_FOUND_NULL;
                            nextBeginOffset = nextOffsetCorrection(nextBeginOffset, consumeQueue.rollNextFile(nextBeginOffset));
                            log.warn("consumer request topic:{}, offset:{}, minOffset:{}, maxOffset:{}, "
                                    + "but access logic queue failed. correct nextBeginOffset to {}",
                                topic, offset, minOffset, maxOffset, nextBeginOffset);
                            break;
                        }

                        try {
                            long nextPhyFileStartOffset = Long.MIN_VALUE;
                            while (bufferConsumeQueue.hasNext() && nextBeginOffset < maxOffset) {
                                CqUnit cqUnit = bufferConsumeQueue.next();
                                if (!validateCqUnit(cqUnit)) {
                                    break;
                                }
                                long offsetPy = cqUnit.getPos();
                                int sizePy = cqUnit.getSize();

                                boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                                if (isTheBatchFull(sizePy, cqUnit.getBatchNum(), maxMsgNums, maxPullSize,
                                    getResult.getBufferTotalSize(), getResult.getMessageCount(), isInDisk)) {
                                    break;
                                }

                                if (getResult.getBufferTotalSize() >= maxPullSize) {
                                    break;
                                }

                                maxPhyOffsetPulling = offsetPy;

                                //Be careful, here should before the isTheBatchFull
                                nextBeginOffset = cqUnit.getQueueOffset() + cqUnit.getBatchNum();

                                if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                    if (offsetPy < nextPhyFileStartOffset) {
                                        continue;
                                    }
                                }

                                SelectMappedBufferResult selectResult = getMessage(offsetPy, sizePy);
                                if (null == selectResult) {
                                    if (getResult.getBufferTotalSize() == 0) {
                                        status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                    }

                                    // nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                    nextPhyFileStartOffset = rollNextFile(offsetPy);
                                    continue;
                                }
                                this.defaultMessageStore.getStoreStatsService().getGetMessageTransferredMsgCount().add(cqUnit.getBatchNum());
                                getResult.addMessage(selectResult, cqUnit.getQueueOffset(), cqUnit.getBatchNum());
                                status = GetMessageStatus.FOUND;
                                nextPhyFileStartOffset = Long.MIN_VALUE;
                            }
                        } finally {
                            bufferConsumeQueue.release();
                        }
                    }

                    long diff = maxOffsetPy - maxPhyOffsetPulling;
                    long memory = (long)(StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                    getResult.setSuggestPullingFromSlave(diff > memory);
                }
            } else {
                status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            }

            if (GetMessageStatus.FOUND == status) {
                this.defaultMessageStore.getStoreStatsService().getGetMessageTimesTotalFound().add(getResult.getMessageCount());
            } else {
                this.defaultMessageStore.getStoreStatsService().getGetMessageTimesTotalMiss().add(getResult.getMessageCount());
            }
            long elapsedTime = this.defaultMessageStore.getSystemClock().now() - beginTime;
            this.defaultMessageStore.getStoreStatsService().setGetMessageEntireTimeMax(elapsedTime);

            getResult.setStatus(status);
            getResult.setNextBeginOffset(nextBeginOffset);
            getResult.setMaxOffset(maxOffset);
            getResult.setMinOffset(minOffset);
            return getResult;
        } finally {
            readMessageLock.unlock();
        }
    }

    ProcessFileList getCompactionFile() {
        List<MappedFile> mappedFileList = Lists.newArrayList(getLog().getMappedFiles());
        if (mappedFileList.size() < 2) {
            return null;
        }

        List<MappedFile> toCompactFiles = mappedFileList.subList(0, mappedFileList.size() - 1);

        //exclude the last writing file
        List<MappedFile> newFiles = Lists.newArrayList();
        for (int i = 0; i < mappedFileList.size() - 1; i++) {
            MappedFile mf = mappedFileList.get(i);
            long maxQueueOffsetInFile = getCQ().getMaxMsgOffsetFromFile(mf.getFile().getName());
            if (maxQueueOffsetInFile > positionMgr.getOffset(topic, queueId)) {
                newFiles.add(mf);
            }
        }

        if (newFiles.isEmpty()) {
            return null;
        }

        return new ProcessFileList(toCompactFiles, newFiles);
    }

    void compactAndReplace(ProcessFileList compactFiles) throws Throwable {
        if (compactFiles == null || compactFiles.isEmpty()) {
            return;
        }

        long startTime = System.nanoTime();
        OffsetMap offsetMap = getOffsetMap(compactFiles.newFiles);
        compaction(compactFiles.toCompactFiles, offsetMap);
        replaceFiles(compactFiles.toCompactFiles, current, compacting);
        positionMgr.setOffset(topic, queueId, offsetMap.lastOffset);
        positionMgr.persist();
        compacting.clean(false, false);
        log.info("this compaction elapsed {} milliseconds",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));

    }

    void doCompaction() {
        if (!state.compareAndSet(State.NORMAL, State.COMPACTING)) {
            log.warn("compactionLog state is {}, skip this time", state.get());
            return;
        }

        try {
            compactAndReplace(getCompactionFile());
        } catch (Throwable e) {
            log.error("do compaction exception: ", e);
        }
        state.compareAndSet(State.COMPACTING, State.NORMAL);
    }

    protected OffsetMap getOffsetMap(List<MappedFile> mappedFileList) throws NoSuchAlgorithmException, DigestException {
        OffsetMap offsetMap = new OffsetMap(offsetMapMemorySize);

        for (MappedFile mappedFile : mappedFileList) {
            Iterator<SelectMappedBufferResult> iterator = mappedFile.iterator(0);
            while (iterator.hasNext()) {
                SelectMappedBufferResult smb = null;
                try {
                    smb = iterator.next();
                    //decode bytebuffer
                    MessageExt msg = MessageDecoder.decode(smb.getByteBuffer(), true, false);
                    if (msg != null) {
                        ////get key & offset and put to offsetMap
                        if (msg.getQueueOffset() > positionMgr.getOffset(topic, queueId)) {
                            offsetMap.put(msg.getKeys(), msg.getQueueOffset());
                        }
                    } else {
                        // msg is null indicate that file is end
                        break;
                    }
                } catch (DigestException e) {
                    log.error("offsetMap put exception: ", e);
                    throw e;
                } finally {
                    if (smb != null) {
                        smb.release();
                    }
                }
            }
        }
        return offsetMap;
    }

    protected void putEndMessage(MappedFileQueue mappedFileQueue) {
        MappedFile lastFile = mappedFileQueue.getLastMappedFile();
        if (!lastFile.isFull()) {
            lastFile.appendMessage(ByteBuffer.allocate(0), endMsgCallback);
        }
    }

    protected void compaction(List<MappedFile> mappedFileList, OffsetMap offsetMap) throws DigestException {
        compacting = new TopicPartitionLog(this, COMPACTING_SUB_FOLDER);

        for (MappedFile mappedFile : mappedFileList) {
            Iterator<SelectMappedBufferResult> iterator = mappedFile.iterator(0);
            while (iterator.hasNext()) {
                SelectMappedBufferResult smb = null;
                try {
                    smb = iterator.next();
                    MessageExt msgExt = MessageDecoder.decode(smb.getByteBuffer(), true, true);
                    if (msgExt == null) {
                        // file end
                        break;
                    } else {
                        checkAndPutMessage(smb, msgExt, offsetMap, compacting);
                    }
                } finally {
                    if (smb != null) {
                        smb.release();
                    }
                }
            }
        }
        putEndMessage(compacting.getLog());
    }

    protected void replaceFiles(List<MappedFile> mappedFileList, TopicPartitionLog current,
        TopicPartitionLog newLog) {

        MappedFileQueue dest = current.getLog();
        MappedFileQueue src = newLog.getLog();

        long beginTime = System.nanoTime();
//        List<String> fileNameToReplace = mappedFileList.stream()
//            .map(m -> m.getFile().getName())
//            .collect(Collectors.toList());

        List<String> fileNameToReplace = dest.getMappedFiles().stream()
            .filter(mappedFileList::contains)
            .map(mf -> mf.getFile().getName())
            .collect(Collectors.toList());

        mappedFileList.forEach(MappedFile::renameToDelete);

        src.getMappedFiles().forEach(mappedFile -> {
            try {
                mappedFile.flush(0);
                mappedFile.moveToParent();
            } catch (IOException e) {
                log.error("move file {} to parent directory exception: ", mappedFile.getFileName());
            }
        });

        dest.getMappedFiles().stream()
            .filter(m -> !mappedFileList.contains(m))
            .forEach(m -> src.getMappedFiles().add(m));

        readMessageLock.lock();
        try {
            mappedFileList.forEach(mappedFile -> mappedFile.destroy(1000));

            dest.getMappedFiles().clear();
            dest.getMappedFiles().addAll(src.getMappedFiles());
            src.getMappedFiles().clear();

            replaceCqFiles(getCQ(), newLog.getCQ(), fileNameToReplace);

            log.info("replace file elapsed {} milliseconds",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beginTime));
        } finally {
            readMessageLock.unlock();
        }
    }

    protected void replaceCqFiles(SparseConsumeQueue currentBcq, SparseConsumeQueue compactionBcq,
        List<String> fileNameToReplace) {
        long beginTime = System.nanoTime();

        MappedFileQueue currentMq = currentBcq.getMappedFileQueue();
        MappedFileQueue compactMq = compactionBcq.getMappedFileQueue();
        List<MappedFile> fileListToDelete = currentMq.getMappedFiles().stream().filter(m ->
            fileNameToReplace.contains(m.getFile().getName())).collect(Collectors.toList());

        fileListToDelete.forEach(MappedFile::renameToDelete);
        compactMq.getMappedFiles().forEach(mappedFile -> {
            try {
                mappedFile.flush(0);
                mappedFile.moveToParent();
            } catch (IOException e) {
                log.error("move consume queue file {} to parent directory exception: ", mappedFile.getFileName(), e);
            }
        });

        currentMq.getMappedFiles().stream()
            .filter(m -> !fileListToDelete.contains(m))
            .forEach(m -> compactMq.getMappedFiles().add(m));

        fileListToDelete.forEach(mappedFile -> mappedFile.destroy(1000));

        currentMq.getMappedFiles().clear();
        currentMq.getMappedFiles().addAll(compactMq.getMappedFiles());
        compactMq.getMappedFiles().clear();

        currentBcq.refresh();
        log.info("replace consume queue file elapsed {} millsecs.",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beginTime));
    }

    public MappedFileQueue getLog() {
        return current.mappedFileQueue;
    }

    public SparseConsumeQueue getCQ() {
        return current.consumeQueue;
    }

//    public SparseConsumeQueue getCompactionScq() {
//        return compactionScq;
//    }

    public void flush(int flushLeastPages) {
        this.flushLog(flushLeastPages);
        this.flushCQ(flushLeastPages);
    }

    public void flushLog(int flushLeastPages) {
        getLog().flush(flushLeastPages);
    }

    public void flushCQ(int flushLeastPages) {
        getCQ().flush(flushLeastPages);
    }

    static class CompactionAppendEndMsgCallback implements CompactionAppendMsgCallback {
        @Override
        public AppendMessageResult doAppend(ByteBuffer bbDest, long fileFromOffset, int maxBlank, ByteBuffer bbSrc) {
            ByteBuffer endInfo = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
            endInfo.putInt(maxBlank);
            endInfo.putInt(BLANK_MAGIC_CODE);
            return new AppendMessageResult(AppendMessageStatus.END_OF_FILE,
                fileFromOffset + bbDest.position(), maxBlank, System.currentTimeMillis());
        }
    }

    static class CompactionAppendMessageCallback implements CompactionAppendMsgCallback {
        private final String topic;
        private final int queueId;
        private final long tagsCode;
        private final long storeTimestamp;

        private final SparseConsumeQueue bcq;
        public CompactionAppendMessageCallback(MessageExt msgExt, SparseConsumeQueue bcq) {
            this.topic = msgExt.getTopic();
            this.queueId =  msgExt.getQueueId();
            this.tagsCode = MessageExtBrokerInner.tagsString2tagsCode(msgExt.getTags());
            this.storeTimestamp = msgExt.getStoreTimestamp();

            this.bcq = bcq;
        }
        public CompactionAppendMessageCallback(String topic, int queueId, long tagsCode, long storeTimestamp, SparseConsumeQueue bcq) {
            this.topic = topic;
            this.queueId =  queueId;
            this.tagsCode = tagsCode;
            this.storeTimestamp = storeTimestamp;

            this.bcq = bcq;
        }

        @Override
        public AppendMessageResult doAppend(ByteBuffer bbDest, long fileFromOffset, int maxBlank, ByteBuffer bbSrc) {

            final int msgLen = bbSrc.getInt(0);
            MappedFile bcqMappedFile = bcq.getMappedFileQueue().getLastMappedFile();
            if (bcqMappedFile.getWrotePosition() + BatchConsumeQueue.CQ_STORE_UNIT_SIZE >= bcqMappedFile.getFileSize()
                || (msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {      //bcq will full or log will full

                bcq.putEndPositionInfo(bcqMappedFile);

                bbDest.putInt(maxBlank);
                bbDest.putInt(BLANK_MAGIC_CODE);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE,
                    fileFromOffset + bbDest.position(), maxBlank, storeTimestamp);
            }

            //get logic offset and physical offset
            int logicOffsetPos = 4 + 4 + 4 + 4 + 4;
            long logicOffset = bbSrc.getLong(logicOffsetPos);
            int destPos = bbDest.position();
            long physicalOffset = fileFromOffset + bbDest.position();
            bbSrc.rewind();
            bbSrc.limit(msgLen);
            bbDest.put(bbSrc);
            bbDest.putLong(destPos + logicOffsetPos + 8, physicalOffset);       //replace physical offset

            boolean result = bcq.putBatchMessagePositionInfo(physicalOffset, msgLen,
                tagsCode, storeTimestamp, logicOffset, (short)1);
            if (!result) {
                log.error("put message {}-{} position info failed", topic, queueId);
            }
            return new AppendMessageResult(AppendMessageStatus.PUT_OK, physicalOffset, msgLen, storeTimestamp);
        }
    }

    static class OffsetMap {
        private final ByteBuffer dataBytes;
        private final int capacity;
        private final int entrySize;
        private int entryNum;
        private final MessageDigest digest;
        private final int hashSize;
        private long lastOffset;
        private final byte[] hash1;
        private final byte[] hash2;

        public OffsetMap(int memorySize) throws NoSuchAlgorithmException {
            this(memorySize, MessageDigest.getInstance("MD5"));
        }

        public OffsetMap(int memorySize, MessageDigest digest) {
            this.hashSize = digest.getDigestLength();
            this.entrySize = hashSize + (Long.SIZE / Byte.SIZE);
            this.capacity = Math.max(memorySize / entrySize, 100);
            this.dataBytes = ByteBuffer.allocate(capacity * entrySize);
            this.hash1 = new byte[hashSize];
            this.hash2 = new byte[hashSize];
            this.entryNum = 0;
            this.digest = digest;
        }

        public void put(String key, final long offset) throws DigestException {
            if (entryNum >= capacity) {
                throw new IllegalArgumentException("offset map is full");
            }
            hashInto(key, hash1);
            int tryNum = 0;
            int index = indexOf(hash1, tryNum);
            while (!isEmpty(index)) {
                dataBytes.position(index);
                dataBytes.get(hash2);
                if (Arrays.equals(hash1, hash2)) {
                    dataBytes.putLong(offset);
                    lastOffset = offset;
                    return;
                }
                tryNum++;
                index = indexOf(hash1, tryNum);
            }

            dataBytes.position(index);
            dataBytes.put(hash1);
            dataBytes.putLong(offset);
            lastOffset = offset;
            entryNum += 1;
        }

        public long get(String key) throws DigestException {
            hashInto(key, hash1);
            int tryNum = 0;
            int maxTryNum = entryNum + hashSize - 4;
            int index = 0;
            do {
                if (tryNum >= maxTryNum) {
                    return -1L;
                }
                index = indexOf(hash1, tryNum);
                dataBytes.position(index);
                if (isEmpty(index)) {
                    return -1L;
                }
                dataBytes.get(hash2);
                tryNum++;
            } while (!Arrays.equals(hash1, hash2));
            return dataBytes.getLong();
        }

        public long getLastOffset() {
            return lastOffset;
        }

        private boolean isEmpty(int pos) {
            return dataBytes.getLong(pos) == 0
                && dataBytes.getLong(pos + 8) == 0
                && dataBytes.getLong(pos + 16) == 0;
        }

        private int indexOf(byte[] hash, int tryNum) {
            int index = readInt(hash, Math.min(tryNum, hashSize - 4)) + Math.max(0, tryNum - hashSize + 4);
            int entry = Math.abs(index) % capacity;
            return entry * entrySize;
        }

        private void hashInto(String key, byte[] buf) throws DigestException {
            digest.update(key.getBytes(StandardCharsets.UTF_8));
            digest.digest(buf, 0, hashSize);
        }

        private int readInt(byte[] buf, int offset) {
            return ((buf[offset] & 0xFF) << 24) |
                ((buf[offset + 1] & 0xFF) << 16) |
                ((buf[offset + 2] & 0xFF) << 8) |
                ((buf[offset + 3] & 0xFF));
        }
    }

    static class TopicPartitionLog {
        MappedFileQueue mappedFileQueue;
        SparseConsumeQueue consumeQueue;

        public TopicPartitionLog(CompactionLog compactionLog) {
            this(compactionLog, null);
        }
        public TopicPartitionLog(CompactionLog compactionLog, String subFolder) {
            if (StringUtils.isBlank(subFolder)) {
                mappedFileQueue = new MappedFileQueue(compactionLog.compactionLogFilePath,
                    compactionLog.compactionLogMappedFileSize, null);
                consumeQueue = new SparseConsumeQueue(compactionLog.topic, compactionLog.queueId,
                    compactionLog.compactionCqFilePath, compactionLog.compactionCqMappedFileSize,
                    compactionLog.defaultMessageStore);
            } else {
                mappedFileQueue = new MappedFileQueue(compactionLog.compactionLogFilePath + File.separator + subFolder,
                    compactionLog.compactionLogMappedFileSize, null);
                consumeQueue = new SparseConsumeQueue(compactionLog.topic, compactionLog.queueId,
                    compactionLog.compactionCqFilePath, compactionLog.compactionCqMappedFileSize,
                    compactionLog.defaultMessageStore, subFolder);
            }
        }

        public void shutdown() {
            mappedFileQueue.shutdown(1000 * 30);
            consumeQueue.getMappedFileQueue().shutdown(1000 * 30);
        }

        public void init(boolean exitOk) throws IOException, RuntimeException {
            if (!mappedFileQueue.load()) {
                shutdown();
                throw new IOException("load log exception");
            }

            if (!consumeQueue.load()) {
                shutdown();
                throw new IOException("load consume queue exception");
            }

            try {
                consumeQueue.recover();
                recover();
                sanityCheck();
            } catch (Exception e) {
                shutdown();
                throw e;
            }
        }

        private void recover() {
            long maxCqPhysicOffset = consumeQueue.getMaxPhyOffsetInLog();
            log.info("{}:{} max physical offset in compaction log is {}",
                consumeQueue.getTopic(), consumeQueue.getQueueId(), maxCqPhysicOffset);
            if (maxCqPhysicOffset > 0) {
                this.mappedFileQueue.setFlushedWhere(maxCqPhysicOffset);
                this.mappedFileQueue.setCommittedWhere(maxCqPhysicOffset);
                this.mappedFileQueue.truncateDirtyFiles(maxCqPhysicOffset);
            }
        }

        void sanityCheck() throws RuntimeException {
            List<MappedFile> mappedFileList = mappedFileQueue.getMappedFiles();
            for (MappedFile file : mappedFileList) {
                if (!consumeQueue.containsOffsetFile(Long.parseLong(file.getFile().getName()))) {
                    throw new RuntimeException("log file mismatch with consumeQueue file " + file.getFileName());
                }
            }

            List<MappedFile> cqMappedFileList = consumeQueue.getMappedFileQueue().getMappedFiles();
            for (MappedFile file: cqMappedFileList) {
                if (mappedFileList.stream().noneMatch(m -> Objects.equals(m.getFile().getName(), file.getFile().getName()))) {
                    throw new RuntimeException("consumeQueue file mismatch with log file " + file.getFileName());
                }
            }
        }

        public synchronized void roll() throws IOException {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            if (mappedFile == null) {
                throw new IOException("create new file error");
            }
            long baseOffset = mappedFile.getFileFromOffset();
            MappedFile cqFile = consumeQueue.createFile(baseOffset);
            if (cqFile == null) {
                mappedFile.destroy(1000);
                mappedFileQueue.getMappedFiles().remove(mappedFile);
                throw new IOException("create new consumeQueue file error");
            }
        }

        public synchronized void roll(int baseOffset) throws IOException {

            MappedFile mappedFile = mappedFileQueue.tryCreateMappedFile(baseOffset);
            if (mappedFile == null) {
                throw new IOException("create new file error");
            }

            MappedFile cqFile = consumeQueue.createFile(baseOffset);
            if (cqFile == null) {
                mappedFile.destroy(1000);
                mappedFileQueue.getMappedFiles().remove(mappedFile);
                throw new IOException("create new consumeQueue file error");
            }
        }

        public boolean isEmptyOrCurrentFileFull() {
            return mappedFileQueue.isEmptyOrCurrentFileFull() ||
                consumeQueue.getMappedFileQueue().isEmptyOrCurrentFileFull();
        }

        public void clean(MappedFileQueue mappedFileQueue) throws IOException {
            for (MappedFile mf : mappedFileQueue.getMappedFiles()) {
                if (mf.getFile().exists()) {
                    log.error("directory {} with {} not empty.", mappedFileQueue.getStorePath(), mf.getFileName());
                    throw new IOException("directory " + mappedFileQueue.getStorePath() + " not empty.");
                }
            }

            mappedFileQueue.destroy();
        }

        public void clean(boolean forceCleanLog, boolean forceCleanCq) throws IOException {
            //clean and delete sub_folder
            if (forceCleanLog) {
                mappedFileQueue.destroy();
            } else {
                clean(mappedFileQueue);
            }

            if (forceCleanCq) {
                consumeQueue.getMappedFileQueue().destroy();
            } else {
                clean(consumeQueue.getMappedFileQueue());
            }
        }

        public MappedFileQueue getLog() {
            return mappedFileQueue;
        }

        public SparseConsumeQueue getCQ() {
            return consumeQueue;
        }
    }

    enum State {
        NORMAL,
        INITIALIZING,
        COMPACTING,
    }

    static class ProcessFileList {
        List<MappedFile> newFiles;
        List<MappedFile> toCompactFiles;
        public ProcessFileList(List<MappedFile> toCompactFiles, List<MappedFile> newFiles) {
            this.toCompactFiles = toCompactFiles;
            this.newFiles = newFiles;
        }

        boolean isEmpty() {
            return CollectionUtils.isEmpty(newFiles) || CollectionUtils.isEmpty(toCompactFiles);
        }
    }

}
