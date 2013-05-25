/**
 * $Id: MessageId.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common;

import java.net.SocketAddress;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class MessageId {
    private SocketAddress address;
    private long offset;


    public MessageId(SocketAddress address, long offset) {
        this.address = address;
        this.offset = offset;
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
