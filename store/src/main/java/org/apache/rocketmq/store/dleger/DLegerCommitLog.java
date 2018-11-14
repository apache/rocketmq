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
package org.apache.rocketmq.store.dleger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.DLegerServer;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.protocol.AppendEntryRequest;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.store.file.DLegerMmapFileStore;
import org.apache.rocketmq.dleger.store.file.MmapFile;
import org.apache.rocketmq.dleger.store.file.MmapFileList;
import org.apache.rocketmq.dleger.store.file.SelectMmapBufferResult;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.StoreStatsService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

/**
 * Store all metadata downtime for recovery, data protection reliability
 */
public class DLegerCommitLog extends CommitLog {
    private final DLegerServer dLegerServer;
    private final DLegerConfig dLegerConfig;
    private final DLegerMmapFileStore dLegerFileStore;
    private final MmapFileList dLegerFileList;

    private final int id;



    private final MessageSerializer messageSerializer;

    private volatile long beginTimeInDlegerLock = 0;

    public DLegerCommitLog(final DefaultMessageStore defaultMessageStore) {
        super(defaultMessageStore);
        dLegerConfig =  new DLegerConfig();
        dLegerConfig.setSelfId(defaultMessageStore.getMessageStoreConfig().getdLegerSelfId());
        dLegerConfig.setGroup(defaultMessageStore.getMessageStoreConfig().getdLegerGroup());
        dLegerConfig.setPeers(defaultMessageStore.getMessageStoreConfig().getdLegerPeers());
        dLegerConfig.setStoreBaseDir(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        id = Integer.valueOf(dLegerConfig.getSelfId().substring(1)) + 1;
        dLegerServer = new DLegerServer(dLegerConfig);
        dLegerFileStore = (DLegerMmapFileStore) dLegerServer.getdLegerStore();
        DLegerMmapFileStore.AppendHook appendHook = (entry, buffer, bodyOffset) -> {
            assert bodyOffset == DLegerEntry.BODY_OFFSET;
            buffer.position(buffer.position() + bodyOffset + MessageDecoder.PHY_POS_POSITION);
            buffer.putLong(entry.getPos() + bodyOffset);
        };
        dLegerFileStore.addAppendHook(appendHook);
        dLegerFileList = dLegerFileStore.getDataFileList();
        this.messageSerializer = new MessageSerializer(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());

    }

    public boolean load() {
        /*boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;*/
        return true;
    }

    public void start() {
        dLegerServer.startup();
       /* this.flushCommitLogService.start();

        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }*/
    }

    public void shutdown() {
       /* if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }
        this.flushCommitLogService.shutdown();*/
        dLegerServer.shutdown();
    }

    public long flush() {
        dLegerFileStore.flush();
        return dLegerFileList.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.dLegerFileList.getMaxWrotePosition();
    }

    public long remainHowManyDataToCommit() {
        return dLegerFileList.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return dLegerFileList.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        return 0;
    }


    private static class DLegerSelectMappedBufferResult extends SelectMappedBufferResult {

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

    public SelectMappedBufferResult convertSbr(SelectMmapBufferResult sbr) {
        if (sbr == null) {
            return null;
        } else {
            return new DLegerSelectMappedBufferResult(sbr);
        }

    }

    public SelectMmapBufferResult truncate(SelectMmapBufferResult sbr) {
        long committedPos = dLegerFileStore.getCommittedPos();
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

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }


    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        if (offset >= dLegerFileStore.getCommittedPos()) {
            return null;
        }
        int mappedFileSize = this.dLegerServer.getdLegerConfig().getMappedFileSizeForEntryData();
        MmapFile mappedFile = this.dLegerFileList.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMmapBufferResult sbr = mappedFile.selectMappedBuffer(pos);
            return  convertSbr(truncate(sbr));
        }

        return null;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     */
    public void recoverNormally() {

    }
    public void recoverAbnormally() {

    }

    public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
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

    public long getConfirmOffset() {
        return this.dLegerFileStore.getCommittedPos() == -1 ? getMaxOffset()
            : this.dLegerFileStore.getCommittedPos();
    }

    public void setConfirmOffset(long phyOffset) {
        log.warn("Should not set confirm offset {} for dleger commitlog", phyOffset);
    }


    private void notifyMessageArriving() {

    }

    public boolean resetOffset(long offset) {
        //return this.mappedFileQueue.resetOffset(offset);
        return false;
    }

    @Override
    public long getBeginTimeInLock() {
        return beginTimeInDlegerLock;
    }

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
            beginTimeInDlegerLock =  this.defaultMessageStore.getSystemClock().now();
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
                request.setGroup(dLegerConfig.getGroup());
                request.setRemoteId(dLegerServer.getMemberState().getSelfId());
                request.setBody(encodeResult.data);
                dlegerFuture = dLegerServer.handleAppend(request);
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
                            DLegerCommitLog.this.topicQueueTable.put(encodeResult.queueOffsetKey, queueOffset + 1);
                            break;
                        default:
                            break;
                    }
                }
            }
            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginTimeInDlegerLock;
        } catch (Exception e) {
            log.error("Put message error", e);
            appendResult = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
        } finally {
            beginTimeInDlegerLock = 0;
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

    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {

    }

    public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {

    }

    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    return result.getByteBuffer().getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    public long getMinOffset() {
        return dLegerFileList.getMinOffset();
    }



    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.dLegerServer.getdLegerConfig().getMappedFileSizeForEntryData();
        MmapFile mappedFile = this.dLegerFileList.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return  convertSbr(mappedFile.selectMappedBuffer(pos, size));
        }
        return null;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void destroy() {
        //TO DO
    }

    public boolean appendData(long startOffset, byte[] data) {
       //TO DO
        return false;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        //TO DO
        return false;
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void checkSelf() {
        dLegerFileList.checkSelf();
    }

    @Override
    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInDlegerLock;
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

            Long queueOffset = DLegerCommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                DLegerCommitLog.this.topicQueueTable.put(key, queueOffset);
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
                DLegerCommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageSize);
                return new EncodeResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED, null, key);
            }
            // Initialization of storage space
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(DLegerCommitLog.MESSAGE_MAGIC_CODE);
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
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 TOPIC
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.msgStoreItemMemory.put(propertiesData);

            final long beginTimeMills = DLegerCommitLog.this.defaultMessageStore.now();

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

    public DLegerServer getdLegerServer() {
        return dLegerServer;
    }

    public int getId() {
        return id;
    }
}
