/**
 * $Id: AppendMessageCallback.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import java.nio.ByteBuffer;


/**
 * 写入消息的回调接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public interface AppendMessageCallback {

    /**
     * 序列化消息后，写入MapedByteBuffer
     * 
     * @param byteBuffer
     *            要写入的target
     * @param maxBlank
     *            要写入的target最大空白区
     * @param msg
     *            要写入的message
     * @return 写入多少字节
     */
    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
            final int maxBlank, final Object msg);
}
