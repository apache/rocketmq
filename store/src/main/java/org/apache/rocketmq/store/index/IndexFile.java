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
 * indexfile结构
 * 40+500W*4+2000W*20= 420000040   即每个index的文件默认大小为420000040b=410,157kb
 * |<—————40b——————>|<—————4*500w——————>|<————————20*2000w—————————>|
 * |————————————————|———————————————————|———————————————————————————|
 * |    header      |   slot table      |   index Linked List       |
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    /**
     * 5000000
     */
    private final int hashSlotNum;
    /**
     * 5000000 * 4
     */
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    /**
     * 处理index文件
     * @param fileName
     * @param hashSlotNum
     * @param indexNum
     * @param endPhyOffset
     * @param endTimestamp
     * @throws IOException
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        /**
         * indexfile结构
         * 40+500W*4+2000W*20= 420000040   即每个index的文件默认大小为420000040b=410,157kb
         * |<—————40b——————>|<—————4*500w——————>|<————————20*2000w—————————>|
         * |————————————————|———————————————————|———————————————————————————|
         * |    header      |   slot table      |   index Linked List       |
         */
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

    /**
     * 读取出index文件中header部分存储的值并缓存
     */
    public void load() {
        this.indexHeader.load();
    }

    /**
     * 刷盘
     */
    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            /**
             * 更新indexHeader
             */
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
     * indexfile结构
     * |<—————40b——————>|<—————4b*500w——————>|<————————20b*2000w—————————>|
     * |————————————————|———————————————————|———————————————————————————|
     * |    header      |   slot table      |   index Linked List       |
     * @param key
     * @param phyOffset
     * @param storeTimestamp
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            /**
             * 计算key对应的hash值
             */
            int keyHash = indexKeyHashMethod(key);
            /**
             * 得到在hashSlotNum中的相应位置
             */
            int slotPos = keyHash % this.hashSlotNum;
            /**
             * 得到SlotPos对应的绝对位置
             */
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                /**
                 * 获取当前位置后面4个字节
                 * 上一个相同keyHash存储在index Linked List的相对位置(this.indexHeader.getIndexCount())
                 */
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                /**
                 * slotValue小于0或者slotValue大于当前index总量  则设置slotValue为0
                 */
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                /**
                 * 计算存储时间与该索引文件的第一个消息(Message)的存储时间(落盘时间)的差值
                 */
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                /**
                 * 得到新增index存储的位置
                 */
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                /**
                 * index存储结构
                 *|————————————————————————————————————————————————————————————|
                 *|   key hash value  |  phyOffset  |  timeDiff  |  prevIndex  |
                 *|        4byte      |    8byte    |    4byte   |    4byte    |
                 *|————————————————————————————————————————————————————————————|
                 */
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);// message key的hash值
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);//message在CommitLog的物理文件地址, 可以直接查询到该消息
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);//message的落盘时间与header里的beginTimestamp的差值(为了节省存储空间，如果直接存message的落盘时间就得8bytes)
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);// hash冲突处理的关键之处, 相同hash值上一个消息索引的index位置

                /**
                 * 更新absSlotPos处的为index在index Linked List中的相对位置，作为下一次该位置新增index数据的前置index相对位置
                 */
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                /**
                 * 第一次
                 */
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                /**
                 * 更新header中的数据
                 */
                this.indexHeader.incHashSlotCount();//+1
                this.indexHeader.incIndexCount();//+1
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
     * 计算key对应的hash
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

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            /**
             * 算出key对应的hash
             */
            int keyHash = indexKeyHashMethod(key);
            /**
             * 在hashSlotNum中的相对位置
             */
            int slotPos = keyHash % this.hashSlotNum;
            /**
             * 计算出slotPos的绝对位置
             */
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                /**
                 * 获取当前位置后面4个字节  即slotValue对应的index
                 */
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        /**
                         * 得到nextIndexToRead对应的index的位置
                         */
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);//上一个相同keyHashRead存储的indexnum

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        /**
                         * 记录keyHash对应的phyOffset
                         */
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        /**
                         * 退出循环
                         * prevIndexRead小于等于0   prevIndexRead大于index总量   prevIndexRead等于当前位置
                         */
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        /**
                         * 查询下一个拥有相同keyHash存储的phyOffset
                         * 向前回溯
                         */
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
