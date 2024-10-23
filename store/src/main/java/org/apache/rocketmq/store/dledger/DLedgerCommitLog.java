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

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageVersion;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageExtEncoder;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.StoreStatsService;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.rocksdb.RocksDBException;

import io.openmessaging.storage.dledger.AppendFuture;
import io.openmessaging.storage.dledger.BatchAppendFuture;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.BatchAppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.file.MmapFile;
import io.openmessaging.storage.dledger.store.file.MmapFileList;
import io.openmessaging.storage.dledger.store.file.SelectMmapBufferResult;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;

/**
 * Store all metadata downtime for recovery, data protection reliability
 */
public class DLedgerCommitLog extends CommitLog {

    static {
        System.setProperty("dLedger.multiPath.Splitter", MessageStoreConfig.MULTI_PATH_SPLITTER);
    }

    private final DLedgerServer dLedgerServer;
    private final DLedgerConfig dLedgerConfig;
    private final DLedgerMmapFileStore dLedgerFileStore;
    private final MmapFileList dLedgerFileList;

    //The id identifies the broker role, 0 means master, others means slave
    private final int id;

    private final MessageSerializer messageSerializer;
    private volatile long beginTimeInDledgerLock = 0;

    //This offset separate the old commitlog from dledger commitlog
    private long dividedCommitlogOffset = -1;

    private boolean isInrecoveringOldCommitlog = false;

    private final StringBuilder msgIdBuilder = new StringBuilder();

    public DLedgerCommitLog(final DefaultMessageStore defaultMessageStore) {
        super(defaultMessageStore);
        dLedgerConfig = new DLedgerConfig();
        dLedgerConfig.setEnableDiskForceClean(defaultMessageStore.getMessageStoreConfig().isCleanFileForciblyEnable());
        dLedgerConfig.setStoreType(DLedgerConfig.FILE);
        dLedgerConfig.setSelfId(defaultMessageStore.getMessageStoreConfig().getdLegerSelfId());
        dLedgerConfig.setGroup(defaultMessageStore.getMessageStoreConfig().getdLegerGroup());
        dLedgerConfig.setPeers(defaultMessageStore.getMessageStoreConfig().getdLegerPeers());
        dLedgerConfig.setStoreBaseDir(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        dLedgerConfig.setDataStorePath(defaultMessageStore.getMessageStoreConfig().getStorePathDLedgerCommitLog());
        dLedgerConfig.setReadOnlyDataStoreDirs(defaultMessageStore.getMessageStoreConfig().getReadOnlyCommitLogStorePaths());
        dLedgerConfig.setMappedFileSizeForEntryData(defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog());
        dLedgerConfig.setDeleteWhen(defaultMessageStore.getMessageStoreConfig().getDeleteWhen());
        dLedgerConfig.setFileReservedHours(defaultMessageStore.getMessageStoreConfig().getFileReservedTime() + 1);
        dLedgerConfig.setPreferredLeaderId(defaultMessageStore.getMessageStoreConfig().getPreferredLeaderId());
        dLedgerConfig.setEnableBatchPush(defaultMessageStore.getMessageStoreConfig().isEnableBatchPush());
        dLedgerConfig.setDiskSpaceRatioToCheckExpired(defaultMessageStore.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100f);

        id = Integer.parseInt(dLedgerConfig.getSelfId().substring(1)) + 1;
        dLedgerServer = new DLedgerServer(dLedgerConfig);
        dLedgerFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
        DLedgerMmapFileStore.AppendHook appendHook = (entry, buffer, bodyOffset) -> {
            assert bodyOffset == DLedgerEntry.BODY_OFFSET;
            buffer.position(buffer.position() + bodyOffset + MessageDecoder.PHY_POS_POSITION);
            buffer.putLong(entry.getPos() + bodyOffset);
        };
        dLedgerFileStore.addAppendHook(appendHook);
        dLedgerFileList = dLedgerFileStore.getDataFileList();
        this.messageSerializer = new MessageSerializer(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());

    }

    @Override
    public boolean load() {
        return super.load();
    }

    private void refreshConfig() {
        dLedgerConfig.setEnableDiskForceClean(defaultMessageStore.getMessageStoreConfig().isCleanFileForciblyEnable());
        dLedgerConfig.setDeleteWhen(defaultMessageStore.getMessageStoreConfig().getDeleteWhen());
        dLedgerConfig.setFileReservedHours(defaultMessageStore.getMessageStoreConfig().getFileReservedTime() + 1);
    }

    private void disableDeleteDledger() {
        dLedgerConfig.setEnableDiskForceClean(false);
        dLedgerConfig.setFileReservedHours(24 * 365 * 10);
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
        if (dLedgerFileStore.getCommittedPos() > 0) {
            return dLedgerFileStore.getCommittedPos();
        }
        if (dLedgerFileList.getMinOffset() > 0) {
            return dLedgerFileList.getMinOffset();
        }
        return 0;
    }

    @Override
    public long getMinOffset() {
        if (!mappedFileQueue.getMappedFiles().isEmpty()) {
            return mappedFileQueue.getMinOffset();
        }
        for (MmapFile file : dLedgerFileList.getMappedFiles()) {
            if (file.isAvailable()) {
                return file.getFileFromOffset() + file.getStartPosition();
            }
        }
        return 0;
    }

    @Override
    public long getConfirmOffset() {
        return this.getMaxOffset();
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
        if (mappedFileQueue.getMappedFiles().isEmpty()) {
            refreshConfig();
            //To prevent too much log in defaultMessageStore
            return Integer.MAX_VALUE;
        } else {
            disableDeleteDledger();
        }
        int count = super.deleteExpiredFile(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
        if (count > 0 || mappedFileQueue.getMappedFiles().size() != 1) {
            return count;
        }
        //the old logic will keep the last file, here to delete it
        MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
        log.info("Try to delete the last old commitlog file {}", mappedFile.getFileName());
        long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
        if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
            while (!mappedFile.destroy(10 * 1000)) {
                DLedgerUtils.sleep(1000);
            }
            mappedFileQueue.getMappedFiles().remove(mappedFile);
        }
        return 1;
    }

    public SelectMappedBufferResult convertSbr(SelectMmapBufferResult sbr) {
        if (sbr == null) {
            return null;
        } else {
            return new DLedgerSelectMappedBufferResult(sbr);
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
        int mappedFileSize = this.dLedgerServer.getdLedgerConfig().getMappedFileSizeForEntryData();
        MmapFile mappedFile = this.dLedgerFileList.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMmapBufferResult sbr = mappedFile.selectMappedBuffer(pos);
            return convertSbr(truncate(sbr));
        }

        return null;
    }

    @Override
    public boolean getData(final long offset, final int size, final ByteBuffer byteBuffer) {
        if (offset < dividedCommitlogOffset) {
            return super.getData(offset, size, byteBuffer);
        }
        if (offset >= dLedgerFileStore.getCommittedPos()) {
            return false;
        }
        int mappedFileSize = this.dLedgerServer.getdLedgerConfig().getMappedFileSizeForEntryData();
        MmapFile mappedFile = this.dLedgerFileList.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.getData(pos, size, byteBuffer);
        }
        return false;
    }

    private void dledgerRecoverNormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        dLedgerFileStore.load();
        if (!dLedgerFileList.getMappedFiles().isEmpty()) {
            dLedgerFileStore.recover();
            dividedCommitlogOffset = dLedgerFileList.getFirstMappedFile().getFileFromOffset();
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                disableDeleteDledger();
            }
            long maxPhyOffset = dLedgerFileList.getMaxWrotePosition();
            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= maxPhyOffset) {
                log.warn("[TruncateCQ]maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, maxPhyOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(maxPhyOffset);
            }
            return;
        }
        //Indicate that, it is the first time to load mixed commitlog, need to recover the old commitlog
        isInrecoveringOldCommitlog = true;
        super.recoverNormally(maxPhyOffsetOfConsumeQueue);
        isInrecoveringOldCommitlog = false;

        setRecoverPosition();

    }

    private void dledgerRecoverAbnormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        dLedgerFileStore.load();
        if (!dLedgerFileList.getMappedFiles().isEmpty()) {
            dLedgerFileStore.recover();
            dividedCommitlogOffset = dLedgerFileList.getFirstMappedFile().getFileFromOffset();
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                disableDeleteDledger();
            }
            List<MmapFile> mmapFiles = dLedgerFileList.getMappedFiles();
            int index = mmapFiles.size() - 1;
            MmapFile mmapFile = null;
            for (; index >= 0; index--) {
                mmapFile = mmapFiles.get(index);
                if (isMmapFileMatchedRecover(mmapFile)) {
                    log.info("dledger recover from this mappFile " + mmapFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mmapFile = mmapFiles.get(index);
            }

            ByteBuffer byteBuffer = mmapFile.sliceByteBuffer();
            long processOffset = mmapFile.getFileFromOffset();
            long mmapFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess()) {
                    if (size > 0) {
                        mmapFileOffset += size;
                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                            if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                                this.defaultMessageStore.doDispatch(dispatchRequest);
                            }
                        } else {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    } else if (size == 0) {
                        index++;
                        if (index >= mmapFiles.size()) {
                            log.info("dledger recover physics file over, last mapped file " + mmapFile.getFileName());
                            break;
                        } else {
                            mmapFile = mmapFiles.get(index);
                            byteBuffer = mmapFile.sliceByteBuffer();
                            processOffset = mmapFile.getFileFromOffset();
                            mmapFileOffset = 0;
                            log.info("dledger recover next physics file, " + mmapFile.getFileName());
                        }
                    }
                } else {
                    log.info("dledger recover physics file end, " + mmapFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }

            processOffset += mmapFileOffset;

            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("dledger maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
            return;
        }
        isInrecoveringOldCommitlog = true;
        super.recoverAbnormally(maxPhyOffsetOfConsumeQueue);

        isInrecoveringOldCommitlog = false;

        setRecoverPosition();

    }

    private void setRecoverPosition() {
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile == null) {
            return;
        }
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        byteBuffer.position(mappedFile.getWrotePosition());
        boolean needWriteMagicCode = true;
        // 1 TOTAL SIZE
        byteBuffer.getInt(); //size
        int magicCode = byteBuffer.getInt();
        if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
            needWriteMagicCode = false;
        } else {
            log.info("Recover old commitlog found a illegal magic code={}", magicCode);
        }
        dLedgerConfig.setEnableDiskForceClean(false);
        dividedCommitlogOffset = mappedFile.getFileFromOffset() + mappedFile.getFileSize();
        log.info("Recover old commitlog needWriteMagicCode={} pos={} file={} dividedCommitlogOffset={}", needWriteMagicCode, mappedFile.getFileFromOffset() + mappedFile.getWrotePosition(), mappedFile.getFileName(), dividedCommitlogOffset);
        if (needWriteMagicCode) {
            byteBuffer.position(mappedFile.getWrotePosition());
            byteBuffer.putInt(mappedFile.getFileSize() - mappedFile.getWrotePosition());
            byteBuffer.putInt(BLANK_MAGIC_CODE);
            mappedFile.flush(0);
        }
        mappedFile.setWrotePosition(mappedFile.getFileSize());
        mappedFile.setCommittedPosition(mappedFile.getFileSize());
        mappedFile.setFlushedPosition(mappedFile.getFileSize());
        dLedgerFileList.getLastMappedFile(dividedCommitlogOffset);
        log.info("Will set the initial commitlog offset={} for dledger", dividedCommitlogOffset);
    }

    private boolean isMmapFileMatchedRecover(final MmapFile mmapFile) {
        ByteBuffer byteBuffer = mmapFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(DLedgerEntry.BODY_OFFSET + MessageDecoder.MESSAGE_MAGIC_CODE_POSITION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        int storeTimestampPosition;
        int sysFlag = byteBuffer.getInt(DLedgerEntry.BODY_OFFSET + MessageDecoder.SYSFLAG_POSITION);
        if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
            storeTimestampPosition = MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION;
        } else {
            // v6 address is 12 byte larger than v4
            storeTimestampPosition = MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION + 12;
        }

        long storeTimestamp = byteBuffer.getLong(DLedgerEntry.BODY_OFFSET + storeTimestampPosition);
        if (storeTimestamp == 0) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("dledger find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("dledger find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;

    }

    @Override
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        dledgerRecoverNormally(maxPhyOffsetOfConsumeQueue);
    }

    @Override
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        dledgerRecoverAbnormally(maxPhyOffsetOfConsumeQueue);
    }

    @Override
    public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo, final boolean readBody) {
        if (isInrecoveringOldCommitlog) {
            return super.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, readBody);
        }
        try {
            int bodyOffset = DLedgerEntry.BODY_OFFSET;
            int pos = byteBuffer.position();
            int magic = byteBuffer.getInt();
            //In dledger, this field is size, it must be gt 0, so it could prevent collision
            int magicOld = byteBuffer.getInt();
            if (magicOld == CommitLog.BLANK_MAGIC_CODE
                || magicOld == MessageDecoder.MESSAGE_MAGIC_CODE
                || magicOld == MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
                byteBuffer.position(pos);
                return super.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, readBody);
            }
            if (magic == MmapFileList.BLANK_MAGIC_CODE) {
                return new DispatchRequest(0, true);
            }
            byteBuffer.position(pos + bodyOffset);
            DispatchRequest dispatchRequest = super.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, readBody);
            if (dispatchRequest.isSuccess()) {
                dispatchRequest.setBufferSize(dispatchRequest.getMsgSize() + bodyOffset);
            } else if (dispatchRequest.getMsgSize() > 0) {
                dispatchRequest.setBufferSize(dispatchRequest.getMsgSize() + bodyOffset);
            }
            return dispatchRequest;
        } catch (Throwable ignored) {
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

    private void setMessageInfo(MessageExtBrokerInner msg, int tranType) {
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));

        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

        setMessageInfo(msg, tranType);

        final String finalTopic = msg.getTopic();

        msg.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        boolean autoMessageVersionOnTopicLen =
            this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        if (autoMessageVersionOnTopicLen && msg.getTopic().length() > Byte.MAX_VALUE) {
            msg.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }

        // Back to Results
        AppendMessageResult appendResult;
        AppendFuture<AppendEntryResponse> dledgerFuture;
        EncodeResult encodeResult;

        encodeResult = this.messageSerializer.serialize(msg);
        if (encodeResult.status != AppendMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL,
                new AppendMessageResult(encodeResult.status)));
        }
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            long elapsedTimeInLock;
            long queueOffset;

            msg.setQueueOffset(this.defaultMessageStore.getQueueStore().getQueueOffset(msg.getTopic(), msg.getQueueId()));
            beginTimeInDledgerLock = this.defaultMessageStore.getSystemClock().now();
            queueOffset = getQueueOffsetByKey(msg, tranType);
            encodeResult.setQueueOffsetKey(queueOffset, false);
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup(dLedgerConfig.getGroup());
            request.setRemoteId(dLedgerServer.getMemberState().getSelfId());
            request.setBody(encodeResult.getData());
            dledgerFuture = (AppendFuture<AppendEntryResponse>) dLedgerServer.handleAppend(request);
            if (dledgerFuture.getPos() == -1) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.OS_PAGE_CACHE_BUSY,
                        new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
            }
            long wroteOffset = dledgerFuture.getPos() + DLedgerEntry.BODY_OFFSET;

            int msgIdLength = (msg.getSysFlag() & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
            ByteBuffer buffer = ByteBuffer.allocate(msgIdLength);

            String msgId = MessageDecoder.createMessageId(buffer, msg.getStoreHostBytes(), wroteOffset);
            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginTimeInDledgerLock;
            appendResult = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, encodeResult.getData().length,
                    msgId, System.currentTimeMillis(), queueOffset, elapsedTimeInLock);

            defaultMessageStore.getQueueStore().increaseQueueOffset(msg.getTopic(), msg.getQueueId(), getMessageNum(msg));
            if (elapsedTimeInLock > 500) {
                log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}",
                    elapsedTimeInLock, msg.getBody().length, appendResult);
            }
        } catch (Exception ex) {
            log.error("Put message error", ex);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        } finally {
            beginTimeInDledgerLock = 0;
            putMessageLock.unlock();
        }

        return dledgerFuture.thenApply(appendEntryResponse -> {
            PutMessageStatus putMessageStatus = PutMessageStatus.UNKNOWN_ERROR;
            switch (DLedgerResponseCode.valueOf(appendEntryResponse.getCode())) {
                case SUCCESS:
                    putMessageStatus = PutMessageStatus.PUT_OK;
                    break;
                case INCONSISTENT_LEADER:
                case NOT_LEADER:
                case LEADER_NOT_READY:
                case DISK_FULL:
                    putMessageStatus = PutMessageStatus.SERVICE_NOT_AVAILABLE;
                    break;
                case WAIT_QUORUM_ACK_TIMEOUT:
                    //Do not return flush_slave_timeout to the client, for the client will ignore it.
                    putMessageStatus = PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH;
                    break;
                case LEADER_PENDING_FULL:
                    putMessageStatus = PutMessageStatus.OS_PAGE_CACHE_BUSY;
                    break;
            }
            PutMessageResult putMessageResult = new PutMessageResult(putMessageStatus, appendResult);
            if (putMessageStatus == PutMessageStatus.PUT_OK) {
                // Statistics
                storeStatsService.getSinglePutMessageTopicTimesTotal(finalTopic).add(1);
                storeStatsService.getSinglePutMessageTopicSizeTotal(msg.getTopic()).add(appendResult.getWroteBytes());
            }
            return putMessageResult;
        });
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        // Set the storage time
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        boolean autoMessageVersionOnTopicLen =
            this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        if (autoMessageVersionOnTopicLen && messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }

        // Back to Results
        AppendMessageResult appendResult;
        BatchAppendFuture<AppendEntryResponse> dledgerFuture;
        EncodeResult encodeResult;

        encodeResult = this.messageSerializer.serialize(messageExtBatch);
        if (encodeResult.status != AppendMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL,
                new AppendMessageResult(encodeResult.status)));
        }

        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config

        try {
            messageExtBatch.setQueueOffset(this.defaultMessageStore.getQueueStore().getQueueOffset(messageExtBatch.getTopic(), messageExtBatch.getQueueId()));

            msgIdBuilder.setLength(0);
            long elapsedTimeInLock;
            long queueOffset;
            int msgNum = 0;

            beginTimeInDledgerLock = this.defaultMessageStore.getSystemClock().now();
            queueOffset = getQueueOffsetByKey(messageExtBatch, tranType);
            encodeResult.setQueueOffsetKey(queueOffset, true);
            BatchAppendEntryRequest request = new BatchAppendEntryRequest();
            request.setGroup(dLedgerConfig.getGroup());
            request.setRemoteId(dLedgerServer.getMemberState().getSelfId());
            request.setBatchMsgs(encodeResult.batchData);
            AppendFuture<AppendEntryResponse> appendFuture = (AppendFuture<AppendEntryResponse>) dLedgerServer.handleAppend(request);
            if (appendFuture.getPos() == -1) {
                log.warn("HandleAppend return false due to error code {}", appendFuture.get().getCode());
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.OS_PAGE_CACHE_BUSY,
                        new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
            }
            dledgerFuture = (BatchAppendFuture<AppendEntryResponse>) appendFuture;

            long wroteOffset = 0;

            int msgIdLength = (messageExtBatch.getSysFlag() & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
            ByteBuffer buffer = ByteBuffer.allocate(msgIdLength);

            boolean isFirstOffset = true;
            long firstWroteOffset = 0;
            for (long pos : dledgerFuture.getPositions()) {
                wroteOffset = pos + DLedgerEntry.BODY_OFFSET;
                if (isFirstOffset) {
                    firstWroteOffset = wroteOffset;
                    isFirstOffset = false;
                }
                String msgId = MessageDecoder.createMessageId(buffer, messageExtBatch.getStoreHostBytes(), wroteOffset);
                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                msgNum++;
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginTimeInDledgerLock;
            appendResult = new AppendMessageResult(AppendMessageStatus.PUT_OK, firstWroteOffset, encodeResult.totalMsgLen,
                    msgIdBuilder.toString(), System.currentTimeMillis(), queueOffset, elapsedTimeInLock);
            appendResult.setMsgNum(msgNum);

            defaultMessageStore.getQueueStore().increaseQueueOffset(messageExtBatch.getTopic(), messageExtBatch.getQueueId(), (short) encodeResult.batchData.size());
            if (elapsedTimeInLock > 500) {
                log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}",
                        elapsedTimeInLock, messageExtBatch.getBody().length, appendResult);
            }
        } catch (Exception re) {
            log.error("Put message error", re);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        } finally {
            beginTimeInDledgerLock = 0;
            putMessageLock.unlock();
        }

        return dledgerFuture.thenApply(appendEntryResponse -> {
            PutMessageStatus putMessageStatus = PutMessageStatus.UNKNOWN_ERROR;
            switch (DLedgerResponseCode.valueOf(appendEntryResponse.getCode())) {
                case SUCCESS:
                    putMessageStatus = PutMessageStatus.PUT_OK;
                    break;
                case INCONSISTENT_LEADER:
                case NOT_LEADER:
                case LEADER_NOT_READY:
                case DISK_FULL:
                    putMessageStatus = PutMessageStatus.SERVICE_NOT_AVAILABLE;
                    break;
                case WAIT_QUORUM_ACK_TIMEOUT:
                    //Do not return flush_slave_timeout to the client, for the client will ignore it.
                    putMessageStatus = PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH;
                    break;
                case LEADER_PENDING_FULL:
                    putMessageStatus = PutMessageStatus.OS_PAGE_CACHE_BUSY;
                    break;
            }
            PutMessageResult putMessageResult = new PutMessageResult(putMessageStatus, appendResult);
            if (putMessageStatus == PutMessageStatus.PUT_OK) {
                // Statistics
                storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).add(appendResult.getMsgNum());
                storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).add(appendResult.getWroteBytes());
            }
            return putMessageResult;
        });
    }

    @Override
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        if (offset < dividedCommitlogOffset) {
            return super.getMessage(offset, size);
        }
        int mappedFileSize = this.dLedgerServer.getdLedgerConfig().getMappedFileSizeForEntryData();
        MmapFile mappedFile = this.dLedgerFileList.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return convertSbr(mappedFile.selectMappedBuffer(pos, size));
        }
        return null;
    }

    @Override
    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    @Override
    public void destroy() {
        super.destroy();
        dLedgerFileList.destroy();
    }

    @Override
    public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
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

    private long getQueueOffsetByKey(MessageExtBrokerInner msg, int tranType) {
        Long queueOffset = msg.getQueueOffset();

        // Transaction messages that require special handling
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
        return queueOffset;
    }

    class EncodeResult {
        private String queueOffsetKey;
        private ByteBuffer data;
        private List<byte[]> batchData;
        private AppendMessageStatus status;
        private int totalMsgLen;

        public EncodeResult(AppendMessageStatus status, ByteBuffer data, String queueOffsetKey) {
            this.data = data;
            this.status = status;
            this.queueOffsetKey = queueOffsetKey;
        }

        public void setQueueOffsetKey(long offset, boolean isBatch) {
            if (!isBatch) {
                this.data.putLong(MessageDecoder.QUEUE_OFFSET_POSITION, offset);
                return;
            }

            for (byte[] data : batchData) {
                ByteBuffer.wrap(data).putLong(MessageDecoder.QUEUE_OFFSET_POSITION, offset++);
            }
        }

        public byte[] getData() {
            return data.array();
        }

        public EncodeResult(AppendMessageStatus status, String queueOffsetKey, List<byte[]> batchData,
            int totalMsgLen) {
            this.batchData = batchData;
            this.status = status;
            this.queueOffsetKey = queueOffsetKey;
            this.totalMsgLen = totalMsgLen;
        }
    }

    class MessageSerializer {

        // The maximum length of the message body
        private final int maxMessageBodySize;

        MessageSerializer(final int size) {
            this.maxMessageBodySize = size;
        }

        public EncodeResult serialize(final MessageExtBrokerInner msgInner) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            long wroteOffset = 0;

            long queueOffset = 0;

            int sysflag = msgInner.getSysFlag();

            int bornHostLength = (sysflag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            String key = msgInner.getTopic() + "-" + msgInner.getQueueId();

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

            final int msgLen = MessageExtEncoder.calMsgLength(msgInner.getVersion(), msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

            ByteBuffer msgStoreItemMemory = ByteBuffer.allocate(msgLen);

            // Exceeds the maximum message
            if (bodyLength > this.maxMessageBodySize) {
                DLedgerCommitLog.log.warn("message body size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageBodySize: " + this.maxMessageBodySize);
                return new EncodeResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED, null, key);
            }
            // Initialization of storage space
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE
            msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            msgStoreItemMemory.putInt(msgInner.getVersion().getMagicCode());
            // 3 BODYCRC
            msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG
            msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            msgStoreItemMemory.putLong(wroteOffset);
            // 8 SYSFLAG
            msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            resetByteBuffer(bornHostHolder, bornHostLength);
            msgStoreItemMemory.put(msgInner.getBornHostBytes(bornHostHolder));
            // 11 STORETIMESTAMP
            msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            resetByteBuffer(storeHostHolder, storeHostLength);
            msgStoreItemMemory.put(msgInner.getStoreHostBytes(storeHostHolder));
            //this.msgBatchMemory.put(msgInner.getStoreHostBytes());
            // 13 RECONSUMETIMES
            msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0) {
                msgStoreItemMemory.put(msgInner.getBody());
            }
            // 16 TOPIC
            msgInner.getVersion().putTopicLength(msgStoreItemMemory, topicLength);
            msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0) {
                msgStoreItemMemory.put(propertiesData);
            }
            return new EncodeResult(AppendMessageStatus.PUT_OK, msgStoreItemMemory, key);
        }

        public EncodeResult serialize(final MessageExtBatch messageExtBatch) {
            String key = messageExtBatch.getTopic() + "-" + messageExtBatch.getQueueId();

            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();

            int totalLength = messagesByteBuff.limit();
            if (totalLength > this.maxMessageBodySize) {
                CommitLog.log.warn("message body size exceeded, msg body size: " + totalLength
                        + ", maxMessageBodySize: " + this.maxMessageBodySize);
                throw new RuntimeException("message size exceeded");
            }

            List<byte[]> batchBody = new LinkedList<>();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                final int msgLen = MessageExtEncoder.calMsgLength(messageExtBatch.getVersion(), messageExtBatch.getSysFlag(), bodyLen, topicLength, propertiesLen);
                ByteBuffer msgStoreItemMemory = ByteBuffer.allocate(msgLen);

                totalMsgLen += msgLen;

                // Initialization of storage space
                this.resetByteBuffer(msgStoreItemMemory, msgLen);
                // 1 TOTALSIZE
                msgStoreItemMemory.putInt(msgLen);
                // 2 MAGICCODE
                msgStoreItemMemory.putInt(messageExtBatch.getVersion().getMagicCode());
                // 3 BODYCRC
                msgStoreItemMemory.putInt(bodyCrc);
                // 4 QUEUEID
                msgStoreItemMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                msgStoreItemMemory.putInt(flag);
                // 6 QUEUEOFFSET
                msgStoreItemMemory.putLong(0L);
                // 7 PHYSICALOFFSET
                msgStoreItemMemory.putLong(0);
                // 8 SYSFLAG
                msgStoreItemMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                msgStoreItemMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                resetByteBuffer(bornHostHolder, bornHostLength);
                msgStoreItemMemory.put(messageExtBatch.getBornHostBytes(bornHostHolder));
                // 11 STORETIMESTAMP
                msgStoreItemMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                resetByteBuffer(storeHostHolder, storeHostLength);
                msgStoreItemMemory.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
                // 13 RECONSUMETIMES
                msgStoreItemMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset
                msgStoreItemMemory.putLong(0);
                // 15 BODY
                msgStoreItemMemory.putInt(bodyLen);
                if (bodyLen > 0) {
                    msgStoreItemMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                }
                // 16 TOPIC
                messageExtBatch.getVersion().putTopicLength(msgStoreItemMemory, topicLength);
                msgStoreItemMemory.put(topicData);
                // 17 PROPERTIES
                msgStoreItemMemory.putShort(propertiesLen);
                if (propertiesLen > 0) {
                    msgStoreItemMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
                }
                byte[] data = new byte[msgLen];
                msgStoreItemMemory.clear();
                msgStoreItemMemory.get(data);
                batchBody.add(data);
            }

            return new EncodeResult(AppendMessageStatus.PUT_OK, key, batchBody, totalMsgLen);
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }
    }

    public static class DLedgerSelectMappedBufferResult extends SelectMappedBufferResult {

        private SelectMmapBufferResult sbr;

        public DLedgerSelectMappedBufferResult(SelectMmapBufferResult sbr) {
            super(sbr.getStartOffset(), sbr.getByteBuffer(), sbr.getSize(), null);
            this.sbr = sbr;
        }

        @Override
        public synchronized void release() {
            super.release();
            if (sbr != null) {
                sbr.release();
            }
        }

    }

    public DLedgerServer getdLedgerServer() {
        return dLedgerServer;
    }

    public int getId() {
        return id;
    }

    public long getDividedCommitlogOffset() {
        return dividedCommitlogOffset;
    }
}
