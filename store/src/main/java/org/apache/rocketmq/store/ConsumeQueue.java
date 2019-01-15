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
package org.apache.rocketmq.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
/**
 * ConsumeQueue存储结构
 * |—————————————————————————————————————————————————————|
 * |   commitlog offset  |     size     |  tag hashcode  |
 * |         8byte       |    4byte     |      8byte     |
 * |—————————————————————————————————————————————————————|
 */
public class ConsumeQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final int CQ_STORE_UNIT_SIZE = 20;
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;

    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    private final ByteBuffer byteBufferIndex;

    private final String storePath;
    private final int mappedFileSize;
    private long maxPhysicOffset = -1;
    private volatile long minLogicOffset = 0;
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        /**
         * 6000000b == 5860kb
         */
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        /**
         * store\consumequeue\{topic}\{queueId}
         */
        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        /**
         * 实例化MappedFileQueue
         */
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    /**
     * 处理store\consumequeue\{topic}\{queueId}下的文件
     * @return
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        /**
         * 默认false
         */
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * 得到当前consumerqueue存储的最大offset  并存储flushedWhere和committedWhere中
     */
    public void recover() {
        /**
         * 获得store/consumequeue/{topic}/{queueid}/下文件列表
         */
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {

            /**
             * 只读取最后三个文件
             */
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            /**
             * 文件大小  6000000b
             */
            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            /**
             * 该文件存储的起始逻辑Offset  每次递增20
             */
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                /**
                 * 遍历读取文件中的offset
                 *
                 * ConsumeQueue存储结构
                 * |—————————————————————————————————————————————————————|
                 * |   commitlog offset  |     size     |  tag hashcode  |
                 * |         8byte       |    4byte     |      8byte     |
                 * |—————————————————————————————————————————————————————|
                 */
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();//commitlog offset 消息的起始物理偏移量physical offset(long 8字节)
                    int size = byteBuffer.getInt();//size 消息大小size(int 4字节)
                    long tagsCode = byteBuffer.getLong();//tag hashcode (long 8字节)

                    if (offset >= 0 && size > 0) {
                        /**
                         * 下一个消息位置
                         */
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        /**
                         * 当前consumerqueue对应的offset
                         */
                        this.maxPhysicOffset = offset;
                        /**
                         * 验证tagsCode是否小于Integer.MIN_VALUE
                         */
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                /**
                 * 文件已读取完毕  即当前文件已经写满
                 */
                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    /**
                     * 列表中最后一个文件已经读取完毕
                     */
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        break;
                    } else {
                        /**
                         * 继续读取下一个consumerqueue文件
                         */
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }

            /**
             * 目前读到的consumerqueue位置
             */
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);

            /**
             * 删除脏文件
             * 假设有两个文件  第一个存储的offset在11到20  第二个存储的offset在21到30   而当前传入的processOffset为15
             * 则保留第一个文件（11-20） 删除第2个文件（21-30）
             */
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    public long getOffsetInQueueByTime(final long timestamp) {
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        long storeTime =
                            this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset =
                                Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                    - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    public void truncateDirtyLogicFiles(long phyOffet) {

        int logicFileSize = this.mappedFileSize;

        this.maxPhysicOffset = phyOffet - 1;
        long maxExtAddr = 1;
        while (true) {
            /**
             * 返回最后一个MappedFile
             */
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                /**
                 * 遍历 每次增加20字节
                 *
                 *
                 * ConsumeQueue存储结构
                 * |—————————————————————————————————————————————————————|
                 * |   commitlog offset  |     size     |  tag hashcode  |
                 * |         8byte       |    4byte     |      8byte     |
                 * |—————————————————————————————————————————————————————|
                 */
                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();//消息的起始物理偏移量physical offset
                    int size = byteBuffer.getInt();//消息大小size
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        /**
                         * consumequeue文件存储的第一条记录   存储的offset大于等于传入的phyOffet
                         * 即consumequeue中的数据大于commitlog中的数据
                         * 所以删除当前consumequeue文件
                         */
                        if (offset >= phyOffet) {
                            /**
                             * 删除mappedFileQueue对应的最后一个MappedFile
                             */
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {

                            /**
                             * consumequeue文件存储的非第一条记录   存储的offset大于等于传入的phyOffset
                             * 即consumequeue中的数据大于commitlog中的数据
                             * 由于不是第一条  所以退出  且不删除该文件
                             */
                            if (offset >= phyOffet) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            /**
                             * 文件读取到最后
                             */
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    /**
     * 刷盘
     * @param flushLeastPages  最少刷几页（内存页   默认一页有4kb数据）
     * @return
     */
    public boolean flush(final int flushLeastPages) {
        /**
         *
         */
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * 设置minLogicOffset
     * @param phyMinOffset
     */
    public void correctMinOffset(long phyMinOffset) {
        /**
         * 获取第一个MappedFile
         */
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            /**
             * 截取mappedByteBuffer从传入位置到最大可写位置之间的buffer
             */
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetPy = result.getByteBuffer().getLong();//commitlog对应的offset
                        result.getByteBuffer().getInt();//size
                        long tagsCode = result.getByteBuffer().getLong();//tags对应的hash

                        if (offsetPy >= phyMinOffset) {
                            /**
                             * 设置queue对应的minLogicOffset
                             * 大于minPhyOffset且最小的一个
                             */
                            this.minLogicOffset = result.getMappedFile().getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        /**
         * 最大重试30次
         */
        final int maxRetries = 30;
        /**
         * 当前consumequeue是否可写
         * NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | WRITE_INDEX_FILE_ERROR_BIT
         */
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }

            /**
             * 向consumequeue写入message
             */
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                /**
                 * 更新checkpoint文件对应的的logicsMsgTimestamp
                 * StoreTimestamp 消息在broker存储时间
                 */
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");
                /**
                 * 睡眠1秒后再次执行
                 */
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        /**
         * 当前consumequeue不能写入
         */
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        /**
         * this.flagBits |= WRITE_LOGICS_QUEUE_ERROR_BIT
         */
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    /**
     *
     * @param offset commitlogoffset
     * @param size
     * @param tagsCode
     * @param cqOffset consumequeueoffset
     * @return
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {

        /**
         * consumequeue已处理过当前offset
         */
        if (offset <= this.maxPhysicOffset) {
            return true;
        }

        /**
         * 有消息写入了commitlog  但没写入consumequeue
         * 组装一条consumequeue得数据
         *
         * ConsumeQueue存储结构
         * |—————————————————————————————————————————————————————|
         * |   commitlog offset  |     size     |  tag hashcode  |
         * |         8byte       |    4byte     |      8byte     |
         * |—————————————————————————————————————————————————————|
         */
        this.byteBufferIndex.flip();//相当于重置
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        /**
         * 当前消息在consumequeue中的偏移量
         */
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        /**
         * 查询最后一个mappedFile  如果已满或不存在则创建
         */
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            /**
             * 文件未写入  且不是第一个consumequeue文件
             */
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);

                /**
                 * 向mappedFile写入expectLogicOffset大小得前置空数据占位
                 */
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                /**
                 * mappedFile目前可写入得offset
                 */
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                /**
                 * expectLogicOffset的位置已经有数据写入
                 */
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                /**
                 * 写入位置不符合预期
                 * 由于之前已经判断expectLogicOffset < currentLogicOffset  所以这里只能为expectLogicOffset > currentLogicOffset
                 * 即预先写入   那么没写过得位置只能为空  无法写入？？？
                 */
                if (expectLogicOffset != currentLogicOffset) {
                    /**
                     * 输出警告
                     */
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }

            /**
             * 当前consumequeue存储得最大commitlog对应得offset
             */
            this.maxPhysicOffset = offset;
            /**
             * 将byteBufferIndex写入filechannel
             */
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    /**
     * 向mappedFile写入untilWhere大小得空数据
     * @param mappedFile
     * @param untilWhere
     */
    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        /**
         * 空数据
         */
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        /**
         * 大小
         */
        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());

        /**
         * 向mappedFile写入空数据
         */
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * 获取startIndex处之后未读的消息
     * @param startIndex
     * @return
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        /**
         * 大于minLogicOffset
         */
        if (offset >= this.getMinLogicOffset()) {
            /**
             * 获取offset所在的MappedFile  如果没有则返回null
             */
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                /**
                 * 截取mappedByteBuffer
                 */
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        /**
         * this.mappedFileQueue.getMaxOffset()   写入消息的最大位置
         */
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
