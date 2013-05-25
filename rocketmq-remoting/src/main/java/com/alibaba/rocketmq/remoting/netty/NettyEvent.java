/**
 * $Id$
 */
package com.alibaba.rocketmq.remoting.netty;

import io.netty.channel.Channel;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class NettyEvent {
    private final NettyEventType type;
    private final String remoteAddr;
    private final Channel channel;


    public NettyEvent(NettyEventType type, String remoteAddr, Channel channel) {
        this.type = type;
        this.remoteAddr = remoteAddr;
        this.channel = channel;
    }


    public NettyEventType getType() {
        return type;
    }


    public String getRemoteAddr() {
        return remoteAddr;
    }


    public Channel getChannel() {
        return channel;
    }


    @Override
    public String toString() {
        return "NettyEvent [type=" + type + ", remoteAddr=" + remoteAddr + ", channel=" + channel + "]";
    }
}
