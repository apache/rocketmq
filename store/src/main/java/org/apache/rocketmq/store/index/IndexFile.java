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


// IndexFile索引文件，其主要设计理念就是为了加速消息的检索性能，根据消息的属性MessageKey快速从CommitLog文件中检索消息 。
// 消息索引文件，主要存储消息 Key 与 Offset 的对应关系。

// 消息消费队列是 RocketMQ专门为消息订阅构建的索引文件，提高根据主题与消息队列检索消息的速度，
// 另外RocketMQ引入了Hash索引机制为消息建立索引， HashMap的设计包含两个基本点: Hash槽与Hash冲突的链表结构。
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum;
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    // IndexHeader 头部，包含 40 个字节，记录该 IndexFile 的统计信息，其结构如下
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



    // RocketMQ 将消息索引键与消息偏移量映射关系写入到IndexFile的实现方法
    // 参数含义 分别为消息索引、消息物理偏移量、 消息存储时间 。
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // Step1: 如果当前已使用条目大于等于允许最大条目数时，则返回 false，表示当前索引文件已写满。
        // 如果当前索引文件未写满，则根据key算出key的hashcode，然后keyHash对hash槽数量取余定位到 hashcode对应的 hash槽下标，
        // hashcode对应的hash槽的物理地址为 IndexHeader头部(40字节)加上下标乘以每个 hash槽的大小(4字节)。
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 则根据key算出key的hashcode
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            // 计算物理地址
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                //这里是 Hash 冲 突链式解决方案的关键实现，
                // Hash 槽中存储的是该 HashCode 所对应的 最新的 Index 条目的 下 标，
                // 新的 Index 条目的最后 4 个字节存储该 HashCode 上一个 条目的 Index下标。
                // 如果Hash槽中存储的值为0或大于当前lndexFile最大条目数或小于-1，
                // 表示该 Hash 槽当前并没有与之对应的 Index 条目。
                // 值得关注的是， IndexFile 条目中存 储的不 是消息索引 key 而是消息属性 key 的 HashCode，
                // 在 根据 key 查找时需要根据消息物理偏移 量找到消息进而再验证消息 key 的值，
                // 之所以只存储 HashCode 而 不存储具体的 key， 是为了将 Index 条 目设计为定长结构，才 能方便地检索 与定位 条目。

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                // Step2:读取hash槽中存储的数据，
                // 如果hash槽存储的数据小于0或大于当前索引文件中的索引条目格式， 则将slotValue设置为 0。
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // Step3 : 计算待存储消息的时间戳与第一条消息时间戳的差值，并转换成秒.
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // Step4: 将条目信息存储在IndexFile中 。
                //  1 ) 计算新添加条目的起始物理偏移量，等于头部字节长度+ hash槽数量 * 单个 hash 槽大小(4个字节)+当前 Index条目个数*单个 Index条目大小(20个字节)。
                //  2 ) 依次将 hashcode、 消息物理偏移量、消息存储时间戳与索引文件时间戳、当前 Hash槽的值存入 MappedByteBuffer 中。
                //  3 ) 将当前 Index 中包含的条目数量存入Hash槽中 ，将覆盖原先Hash槽的值。
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // Steps : 更新文件索引头信息 。
                // 如果当前文件只包含一个条目，更新 beginPhyOffset与 beginTimestamp、更新 endPyhOffset、 endTimestamp、当前文件使用索引条目等信息。
                // RocketMQ 根据索引key查找消息的实现方法为:
                // selectPhyOffset(List<Long> phyOffset，
                // StringKey,
                // int maxNumLongBegin, long end)，
                // 其参数说如下:
                // List<Long> phyOffsets: 查找到的消息物理偏移量 。
                // StringKey: 索引key。
                // int maxNum: 本次查找最大消息条数 。
                // long begin : 开始时间戳 。
                // long end:结束时间戳;
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

    // List<Long> phyOffsets: 查找到的消息物理偏移量 。
    // StringKey: 索引key。
    // int maxNum: 本次查找最大消息条数 。
    // long begin : 开始时间戳 。
    // long end:结束时 间戳 。
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            //1、跟生成索引时一样，找到key的slot
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                //2、获取该槽位上的最后一条索引的序号
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


                        // 3、找到index的位置
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        // 4、在commitLog中偏移
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        // 5、相同 hashcode 的前一条消息的序号
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        //6、Hash和time都符合条件，加入返回列表
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        //7、前一条不等于0，继续读入前一条;如果存在相同hash的前一条index，并且返回列表没到最大值，则继续向前搜索
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
