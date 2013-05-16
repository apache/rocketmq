/**
 * $Id: IndexHeader.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store.index;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 索引文件头
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class IndexHeader {
    public static final int INDEX_HEADER_SIZE = 40;

    private AtomicLong beginTimestamp = new AtomicLong(0);
    private AtomicLong endTimestamp = new AtomicLong(0);
    private AtomicLong beginPhyOffset = new AtomicLong(0);
    private AtomicLong endPhyOffset = new AtomicLong(0);
    private AtomicInteger hashSlotCount = new AtomicInteger(0);
    // 第一个索引是无效索引
    private AtomicInteger indexCount = new AtomicInteger(1);

    private static int BEGINTIMESTAMP_INDEX = 0;
    private static int ENDTIMESTAMP_INDEX = 8;
    private static int BEGINPHYOFFSET_INDEX = 16;
    private static int ENDPHYOFFSET_INDEX = 24;
    private static int HASHSLOTCOUNT_INDEX = 32;
    private static int INDEXCOUNT_INDEX = 36;

    private final ByteBuffer byteBuffer;


    public IndexHeader(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }


    public void load() {
        this.beginTimestamp.set(byteBuffer.getLong(BEGINTIMESTAMP_INDEX));
        this.endTimestamp.set(byteBuffer.getLong(ENDTIMESTAMP_INDEX));
        this.beginPhyOffset.set(byteBuffer.getLong(BEGINPHYOFFSET_INDEX));
        this.endPhyOffset.set(byteBuffer.getLong(ENDPHYOFFSET_INDEX));

        this.hashSlotCount.set(byteBuffer.getInt(HASHSLOTCOUNT_INDEX));
        this.indexCount.set(byteBuffer.getInt(INDEXCOUNT_INDEX));

        if (this.indexCount.get() <= 0) {
            this.indexCount.set(1);
        }
    }


    /**
     * 更新byteBuffer
     */
    public void updateByteBuffer() {
        this.byteBuffer.putLong(BEGINTIMESTAMP_INDEX, this.beginTimestamp.get());
        this.byteBuffer.putLong(ENDTIMESTAMP_INDEX, this.endTimestamp.get());
        this.byteBuffer.putLong(BEGINPHYOFFSET_INDEX, this.beginPhyOffset.get());
        this.byteBuffer.putLong(ENDPHYOFFSET_INDEX, this.endPhyOffset.get());
        this.byteBuffer.putInt(HASHSLOTCOUNT_INDEX, this.hashSlotCount.get());
        this.byteBuffer.putInt(INDEXCOUNT_INDEX, this.indexCount.get());
    }


    public long getBeginTimestamp() {
        return beginTimestamp.get();
    }


    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp.set(beginTimestamp);
        this.byteBuffer.putLong(BEGINTIMESTAMP_INDEX, beginTimestamp);
    }


    public long getEndTimestamp() {
        return endTimestamp.get();
    }


    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp.set(endTimestamp);
        this.byteBuffer.putLong(ENDTIMESTAMP_INDEX, endTimestamp);
    }


    public long getBeginPhyOffset() {
        return beginPhyOffset.get();
    }


    public void setBeginPhyOffset(long beginPhyOffset) {
        this.beginPhyOffset.set(beginPhyOffset);
        this.byteBuffer.putLong(BEGINPHYOFFSET_INDEX, beginPhyOffset);
    }


    public long getEndPhyOffset() {
        return endPhyOffset.get();
    }


    public void setEndPhyOffset(long endPhyOffset) {
        this.endPhyOffset.set(endPhyOffset);
        this.byteBuffer.putLong(ENDPHYOFFSET_INDEX, endPhyOffset);
    }


    public AtomicInteger getHashSlotCount() {
        return hashSlotCount;
    }


    public void incHashSlotCount() {
        int value = this.hashSlotCount.incrementAndGet();
        this.byteBuffer.putInt(HASHSLOTCOUNT_INDEX, value);
    }


    public int getIndexCount() {
        return indexCount.get();
    }


    public void incIndexCount() {
        int value = this.indexCount.incrementAndGet();
        this.byteBuffer.putInt(INDEXCOUNT_INDEX, value);
    }
}
