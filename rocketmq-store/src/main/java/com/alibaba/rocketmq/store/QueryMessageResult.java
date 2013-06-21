/**
 * $Id: QueryMessageResult.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class QueryMessageResult {
    private long indexLastUpdateTimestamp;
    private long indexLastUpdatePhyoffset;
    // 多个连续的消息集合
    private final List<SelectMapedBufferResult> messageMapedList =
            new ArrayList<SelectMapedBufferResult>(100);
    // 用来向Consumer传送消息
    private final List<ByteBuffer> messageBufferList = new ArrayList<ByteBuffer>(100);
    // ByteBuffer 总字节数
    private int bufferTotalSize = 0;


    public void addMessage(final SelectMapedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
    }


    public void release() {
        for (SelectMapedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }


    public long getIndexLastUpdateTimestamp() {
        return indexLastUpdateTimestamp;
    }


    public void setIndexLastUpdateTimestamp(long indexLastUpdateTimestamp) {
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
    }


    public long getIndexLastUpdatePhyoffset() {
        return indexLastUpdatePhyoffset;
    }


    public void setIndexLastUpdatePhyoffset(long indexLastUpdatePhyoffset) {
        this.indexLastUpdatePhyoffset = indexLastUpdatePhyoffset;
    }


    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }


    public int getBufferTotalSize() {
        return bufferTotalSize;
    }
}
