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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * 用于为生成的索引文件提供访问服务，通过消息Key值查询消息真正的实体内容。
 * 在实际的物理存储上，文件名则是以创建时的时间戳命名的，固定的单个IndexFile文件大小约为400M，
 * 一个IndexFile可以保存 2000W个索引；
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    //Hash槽，一个IndexFile默认包含500万个Hash槽，每个Hash槽存储的是落在该槽的hashcode最新的Index的索引
    private final int hashSlotNum;
    //最大条目数
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    //缓冲区
    private final MappedByteBuffer mappedByteBuffer;
    //索引头部，包含40个字节，记录IndexFile的统计信息
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 保存索引信息
     * @param key 消息索引
     * @param phyOffset 消息物理偏移量
     * @param storeTimestamp 消息存储时间
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        //如果当前已使用条目大于等于允许最大条目数时，返回false，表示当前索引文件已写满
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            //计算出key的hashcode
            int keyHash = indexKeyHashMethod(key);
            //然后根据key的hashcode对hash槽数取余定位到hashcode对应的hash槽下标
            int slotPos = keyHash % this.hashSlotNum;
            //hash槽下标对应的物理位置为IndexHeader头部（40字节）+下标*每个槽位的大小（4字节）
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                //读取hash槽中存储的数据
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                //如果hash槽存储的数据小于0，或者大于当前索引允许最大条目数，
                // 表示该槽位并没有与之对应的Index条目，将槽数据设置为0
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }
                //计算待存储消息的时间戳与第一条消息时间戳的差值，并转为秒
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                //将条目信息存储在IndexFile中
                //计算新添条目物理偏移量，等于 头部字节长度+hash槽数量*单个槽大小+当前Index条目个数*单个条目大小
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                //key的hashcode（4字节）
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                //消息对应的物理偏移量（8字节）
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                //该消息存储时间与第一条消息的时间戳的差值，小于0该消息无效（4字节）
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                //当前hash槽的值，即当key hash冲突时，Index条目会被分配到同一个槽，此时会构建链表，
                // 将新的Index条目数存放入槽中，旧的Index条目数存在新的Index条目尾部4字节中，新旧形成链表结构
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                //将当前Index中包含的条目数量存入Hash槽中，将覆盖原先Hash槽的值
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                //更新文件索引头部信息
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    /**
     * 计算key的hashcode
     * @param key
     * @return
     */
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 根据key查找消息
     * @param phyOffsets 查找到的消息物理偏移量
     * @param key 索引key
     * @param maxNum 本次查找最大消息条目数
     * @param begin  开始时间戳
     * @param end 结束时间戳
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            //根据key计算hashcode
            int keyHash = indexKeyHashMethod(key);
            //定位槽下标
            int slotPos = keyHash % this.hashSlotNum;
            //计算对应槽的物理偏移量
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

                //槽中值小于0，或大于最大允许条目数，或小于最大允许条目数小于等于1，说明没有对应的条目
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    //由于存在hash冲突，根据slotValue定位该hash槽最新的一个Item条目
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        //根据Index下标计算条目起始物理偏移量
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        //如果时间戳差值小于0，则消息无效
                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        //如果hashcode匹配且消息存储时间介于查找时间之间，将物理偏移量存入list
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        //当前一个Index索引小于等于0，或大于最大条目数，或等于当前索引，或存储时间小于开始时间，则跳出链表遍历循环
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        //链表下查
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
