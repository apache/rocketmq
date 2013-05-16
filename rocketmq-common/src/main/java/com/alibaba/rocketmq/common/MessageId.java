/**
 * $Id: MessageId.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common;

import java.net.SocketAddress;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class MessageId {
    private long timestamp;
    private SocketAddress address;
    private long offset;


    public MessageId(long timestamp, SocketAddress address, long offset) {
        this.timestamp = timestamp;
        this.address = address;
        this.offset = offset;
    }


    public long getTimestamp() {
        return timestamp;
    }


    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


    public SocketAddress getAddress() {
        return address;
    }


    public void setAddress(SocketAddress address) {
        this.address = address;
    }


    public long getOffset() {
        return offset;
    }


    public void setOffset(long offset) {
        this.offset = offset;
    }
}
