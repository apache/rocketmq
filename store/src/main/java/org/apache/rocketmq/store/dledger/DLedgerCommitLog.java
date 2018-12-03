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
package org.apache.rocketmq.store.dledger;

import io.openmessaging.storage.dleger.DLegerConfig;
import io.openmessaging.storage.dleger.DLegerServer;
import io.openmessaging.storage.dleger.entry.DLegerEntry;
import io.openmessaging.storage.dleger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dleger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dleger.protocol.DLegerResponseCode;
import io.openmessaging.storage.dleger.store.file.DLegerMmapFileStore;
import io.openmessaging.storage.dleger.store.file.MmapFile;
import io.openmessaging.storage.dleger.store.file.MmapFileList;
import io.openmessaging.storage.dleger.store.file.SelectMmapBufferResult;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.StoreStatsService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

/**
 * Store all metadata downtime for recovery, data protection reliability
 */
public class DLedgerCommitLog extends CommitLog {
    private final DLegerServer dLedgerServer;
    private final DLegerConfig dLedgerConfig;
    private final DLegerMmapFileStore dLedgerFileStore;
    private final MmapFileList dLedgerFileList;

    //The id identifies the broker role, 0 means master, others means slave
    private final int id;

    private final MessageSerializer messageSerializer;
    private volatile long beginTimeInDledgerLock = 0;

    //This offset separate the old commitlog from dledger commitlog
    private long dividedCommitlogOffset = -1;


    //The old commitlog should be deleted before the dledger commitlog
    private final boolean originalDledgerEnableForceClean;

    public DLedgerCommitLog(final DefaultMessageStore defaultMessageStore) {
        super(defaultMessageStore);
        dLedgerConfig =  new DLegerConfig();
        dLedgerConfig.setEnableDiskForceClean(defaultMessageStore.getMessageStoreConfig().isCleanFileForciblyEnable());
        dLedgerConfig.setSelfId(defaultMessageStore.getMessageStoreConfig().getdLegerSelfId());
        dLedgerConfig.setGroup(defaultMessageStore.getMessageStoreConfig().getdLegerGroup());
        dLedgerConfig.setPeers(defaultMessageStore.getMessageStoreConfig().getdLegerPeers());
        dLedgerConfig.setStoreBaseDir(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        dLedgerConfig.setMappedFileSizeForEntryData(defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog());
        originalDledgerEnableForceClean =  dLedgerConfig.isEnableDiskForceClean();
        id = Integer.valueOf(dLedgerConfig.getSelfId().substring(1)) + 1;
        dLedgerServer = new DLegerServer(dLedgerConfig);
        dLedgerFileStore = (DLegerMmapFileStore) dLedgerServer.getdLegerStore();
        DLegerMmapFileStore.AppendHook appendHook = (entry, buffer, bodyOffset) -> {
            assert bodyOffset == DLegerEntry.BODY_OFFSET;
            buffer.position(buffer.position() + bodyOffset + MessageDecoder.PHY_POS_POSITION);
            buffer.putLong(entry.getPos() + bodyOffset);
        };
        dLedgerFileStore.addAppendHook(appendHook);
        dLedgerFileList = dLedgerFileStore.getDataFileList();
        this.messageSerializer = new MessageSerializer(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());

    }

    @Override
    public boolean load() {
        boolean result = super.load();
        if (!result) {
            return false;
        }
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile == null) {
            return true;
        }
        dLedgerConfig.setEnableDiskForceClean(false);
        ByteBuffer byteBuffer =  mappedFile.sliceByteBuffer();
        int currentPos = 0;
        boolean needWriteMagicCode = true;
        while (true) {
            byteBuffer.position(currentPos);
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();
            int magicCode = byteBuffer.getInt();
            if (magicCode == BLANK_MAGIC_CODE) {
                needWriteMagicCode = false;
                break;
            }
            if (magicCode != MESSAGE_MAGIC_CODE) {
                log.info("Recover old commitlog found a illegal magic code={}", magicCode);
                break;
            }
            currentPos = currentPos + totalSize;
        }
        log.info("Recover old commitlog needWriteMagicCode={} pos={} file={}", needWriteMagicCode, currentPos, mappedFile.getFileName());
        if (needWriteMagicCode) {
            byteBuffer.position(currentPos);
            byteBuffer.putInt(mappedFile.getFileSize() - currentPos);
            byteBuffer.putInt(BLANK_MAGIC_CODE);
            mappedFile.flush(0);
        }
        dividedCommitlogOffset = mappedFile.getFileFromOffset() + mappedFile.getFileSize();
        if (dLedgerFileList.getMappedFiles().isEmpty()) {
            log.info("Recover to set the initial offset the dledger commitlog dividedCommitlogOffset={}", dividedCommitlogOffset);
            dLedgerFileList.getLastMappedFile(dividedCommitlogOffset);
        }
        return true;
    }

    @Override
    public void start() {
        dLedgerServer.startup();
    }

    @Override
    public void shutdown() {
        dLedgerServer.shutdown();
    }

    @Override
    public long flush() {
        dLedgerFileStore.flush();
        return dLedgerFileList.getFlushedWhere();
    }

    @Override
    public long getMaxOffset() {
        if (this.dLedgerFileStore.getCommittedPos() != -1) {
            return dLedgerFileStore.getCommittedPos();
        } else {
            return this.dLedgerFileList.getMaxWrotePosition();
        }
    }

    @Override
    public long getMinOffset() {
        if (mappedFileQueue.getMappedFiles().isEmpty()) {
            return dLedgerFileList.getMinOffset();
        }
        return mappedFileQueue.getMinOffset();
    }

    @Override
    public long getConfirmOffset() {
        return this.dLedgerFileStore.getCommittedPos() == -1 ? getMaxOffset()
            : this.dLedgerFileStore.getCommittedPos();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        log.warn("Should not set confirm offset {} for dleger commitlog", phyOffset);
    }



    @Override
    public long remainHowManyDataToCommit() {
        return dLedgerFileList.remainHowManyDataToCommit();
    }

    @Override
    public long remainHowManyDataToFlush() {
        return dLedgerFileList.remainHowManyDataToFlush();
    }

    @Override
    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        int count = super.deleteExpiredFile(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
        if (mappedFileQueue.getMappedFiles().isEmpty()) {
            dLedgerConfig.setEnableDiskForceClean(originalDledgerEnableForceClean);
        }
        //return 1 to prevent too much log in defaultMessageStore
        return count > 0 ? count : 1;
    }


    public SelectMappedBufferResult convertSbr(SelectMmapBufferResult sbr) {
        if (sbr == null) {
            return null;
        } else {
            return new DLegerSelectMappedBufferResult(sbr);
        }

    }

    public SelectMmapBufferResult truncate(SelectMmapBufferResult sbr) {
        long committedPos = dLedgerFileStore.getCommittedPos();
        if (sbr == null || sbr.getStartOffset() == committedPos) {
            return null;
        }
        if (sbr.getStartOffset() + sbr.getSize() <= committedPos) {
            return sbr;
        } else {
            sbr.setSize((int) (committedPos - sbr.getStartOffset()));
            return sbr;
        }
    }

    @Override
    public SelectMappedBufferResult getData(final long offset) {
        if (offset < dividedCommitlogOffset) {
            return super.getData(offset);
        }
        return this.getData(offset, offset == 0);
    }


    @Override
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        if (offset < dividedCommitlogOffset) {
            return super.getData(offset, returnFirstOnNotFound);
        }
        if (offset >= dLedgerFileStore.getCommittedPos()) {
            return null;
        }
        int mappedFileSize = this.dLedgerServer.getdLegerConfig().getMappedFileSizeForEntryData();
        MmapFile mappedFile = this.dLedgerFileList.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMmapBufferResult sbr = mappedFile.selectMappedBuffer(pos);
            return  convertSbr(truncate(sbr));
        }

        return null;
    }

    @Override
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue)  {
        if (dLedgerFileList.getMappedFiles().isEmpty()) {
            super.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }
    }

    @Override
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        if (dLedgerFileList.getMappedFiles().isEmpty()) {
            super.recoverNormally(maxPhyOffsetOfConsumeQueue);
        }
    }

    @Override
    public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    @Override
    public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean readBody) {
        try {
            int bodyOffset = DLegerEntry.BODY_OFFSET;
            int pos = byteBuffer.position();
            int magic =  byteBuffer.getInt();
            if (magic == MmapFileList.BLANK_MAGIC_CODE) {
                return new DispatchRequest(0, true);
            } else {
                byteBuffer.position(pos + bodyOffset);
                DispatchRequest dispatchRequest = super.checkMessageAndReturnSize(byteBuffer, checkCRC, readBody);
                if (dispatchRequest.isSuccess()) {
                    dispatchRequest.setBufferSize(dispatchRequest.getMsgSize() + bodyOffset);
                } else if (dispatchRequest.getMsgSize() > 0) {
                    dispatchRequest.setBufferSize(dispatchRequest.getMsgSize() + bodyOffset);
                }
                return dispatchRequest;
            }
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    @Override
    public boolean resetOffset(long offset) {
        //currently, it seems resetOffset has no use
        return false;
    }

    @Override
    public long getBeginTimeInLock() {
        return beginTimeInDledgerLock;
    }

    @Override
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        //should be consistent with the old version
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
            || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            if (msg.getDelayTimeLevel() > 0) {
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        // Back to Results
        AppendMessageResult appendResult = null;
        PutMessageStatus putMessageStatus = PutMessageStatus.UNKNOWN_ERROR;
        CompletableFuture<AppendEntryResponse> dlegerFuture = null;
        EncodeResult encodeResult = null;
        long eclipseTimeInLock = 0L;
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        long queueOffset = -1;
        try {
            beginTimeInDledgerLock =  this.defaultMessageStore.getSystemClock().now();
            //TO DO use buffer
            encodeResult = this.messageSerializer.serialize(msg);
            queueOffset = topicQueueTable.get(encodeResult.queueOffsetKey);
            if (encodeResult.status  != AppendMessageStatus.PUT_OK) {
                appendResult = new AppendMessageResult(encodeResult.status);
                switch (encodeResult.status) {
                    case PROPERTIES_SIZE_EXCEEDED:
                    case MESSAGE_SIZE_EXCEEDED:
                        putMessageStatus = PutMessageStatus.MESSAGE_ILLEGAL;
                        break;
                }
            } else {
                AppendEntryRequest request = new AppendEntryRequest();
                request.setGroup(dLedgerConfig.getGroup());
                request.setRemoteId(dLedgerServer.getMemberState().getSelfId());
                request.setBody(encodeResult.data);
                dlegerFuture = dLedgerServer.handleAppend(request);
                if (dlegerFuture.isDone() && dlegerFuture.get().getCode() != DLegerResponseCode.SUCCESS.getCode()) {
                    //TO DO make sure the local store is ok
                    appendResult = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                } else {
                    switch (tranType) {
                        case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                        case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                            break;
                        case MessageSysFlag.TRANSACTION_NOT_TYPE:
                        case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                            // The next update ConsumeQueue information
                            DLedgerCommitLog.this.topicQueueTable.put(encodeResult.queueOffsetKey, queueOffset + 1);
                            break;
                        default:
                            break;
                    }
                }
            }
            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginTimeInDledgerLock;
        } catch (Exception e) {
            log.error("Put message error", e);
            appendResult = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
        } finally {
            beginTimeInDledgerLock = 0;
            putMessageLock.unlock();
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, msg.getBody().length, appendResult);
        }

        if (dlegerFuture != null) {
            try {
                AppendEntryResponse appendEntryResponse = dlegerFuture.get(3, TimeUnit.SECONDS);
                switch (DLegerResponseCode.valueOf(appendEntryResponse.getCode())) {
                    case SUCCESS:
                        putMessageStatus = PutMessageStatus.PUT_OK;
                        long wroteOffset =  appendEntryResponse.getPos() + DLegerEntry.BODY_OFFSET;
                        ByteBuffer buffer = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
                        String msgId = MessageDecoder.createMessageId(buffer, msg.getStoreHostBytes(), wroteOffset);
                        appendResult = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, encodeResult.data.length, msgId, System.currentTimeMillis(), queueOffset, 0);
                        break;
                    case INCONSISTENT_LEADER:
                    case NOT_LEADER:
                    case LEADER_NOT_READY:
                    case DISK_FULL:
                        putMessageStatus = PutMessageStatus.SERVICE_NOT_AVAILABLE;
                        break;
                    case WAIT_QUORUM_ACK_TIMEOUT:
                        putMessageStatus = PutMessageStatus.FLUSH_SLAVE_TIMEOUT;
                        break;
                    case LEADER_PENDING_FULL:
                        putMessageStatus = PutMessageStatus.OS_PAGECACHE_BUSY;
                        break;
                }
            } catch (Exception ignored) {
                putMessageStatus = PutMessageStatus.FLUSH_SLAVE_TIMEOUT;
                appendResult = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
        }

        PutMessageResult putMessageResult = new PutMessageResult(putMessageStatus, appendResult);
        if (putMessageStatus == PutMessageStatus.PUT_OK) {
            // Statistics
            storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
            storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(appendResult.getWroteBytes());
        }
        return putMessageResult;
    }

    @Override
    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
    }



    @Override
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        if (offset < dividedCommitlogOffset) {
            return getMessage(offset, size);
        }
        int mappedFileSize = this.dLedgerServer.getdLegerConfig().getMappedFileSizeForEntryData();
        MmapFile mappedFile = this.dLedgerFileList.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return  convertSbr(mappedFile.selectMappedBuffer(pos, size));
        }
        return null;
    }

    @Override
    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    @Override
    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    @Override
    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    @Override
    public void destroy() {
        super.destroy();
        dLedgerFileList.destroy();
    }

    @Override
    public boolean appendData(long startOffset, byte[] data) {
        //the old ha service will invoke method, here to prevent it
        return false;
    }

    @Override
    public void checkSelf() {
        dLedgerFileList.checkSelf();
    }

    @Override
    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInDledgerLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    class EncodeResult {
        private String queueOffsetKey;
        private byte[] data;
        private AppendMessageStatus status;
        public EncodeResult(AppendMessageStatus status, byte[] data, String queueOffsetKey) {
            this.data = data;
            this.status = status;
            this.queueOffsetKey = queueOffsetKey;
        }
    }

    class MessageSerializer {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        private final ByteBuffer msgIdMemory;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;
        // The maximum length of the message
        private final int maxMessageSize;
        // Build Message Key
        private final StringBuilder keyBuilder = new StringBuilder();

        private final StringBuilder msgIdBuilder = new StringBuilder();

        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        MessageSerializer(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }

        public EncodeResult serialize(final MessageExtBrokerInner msgInner) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            long wroteOffset = 0;

            this.resetByteBuffer(hostHolder, 8);
            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());
            String key = keyBuilder.toString();

            Long queueOffset = DLedgerCommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                DLedgerCommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // Transaction messages that require special handling
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queuec
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            /**
             * Serialize message
             */
            final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new EncodeResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED, null, key);
            }

            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            final int msgLen = calMsgLength(bodyLength, topicLength, propertiesLength);

            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize) {
                DLedgerCommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageSize);
                return new EncodeResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED, null, key);
            }
            // Initialization of storage space
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(DLedgerCommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            this.msgStoreItemMemory.putLong(wroteOffset);
            // 8 SYSFLAG
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(hostHolder));
            // 11 STORETIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(hostHolder));
            //this.msgBatchMemory.put(msgInner.getStoreHostBytes());
            // 13 RECONSUMETIMES
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0) {
                this.msgStoreItemMemory.put(msgInner.getBody());
            }
            // 16 TOPIC
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0) {
                this.msgStoreItemMemory.put(propertiesData);
            }
            byte[] data = new byte[msgLen];
            this.msgStoreItemMemory.clear();
            this.msgStoreItemMemory.get(data);
            return new EncodeResult(AppendMessageStatus.PUT_OK, data, key);
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    static class DLegerSelectMappedBufferResult extends SelectMappedBufferResult {

        private SelectMmapBufferResult sbr;
        public DLegerSelectMappedBufferResult(SelectMmapBufferResult sbr) {
            super(sbr.getStartOffset(), sbr.getByteBuffer(), sbr.getSize(), null);
            this.sbr = sbr;
        }

        public synchronized void release() {
            super.release();
            sbr.release();
        }

    }

    public DLegerServer getdLedgerServer() {
        return dLedgerServer;
    }

    public int getId() {
        return id;
    }

    public long getDividedCommitlogOffset() {
        return dividedCommitlogOffset;
    }
}
