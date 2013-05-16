package com.alibaba.rocketmq.remoting;

import io.netty.channel.Channel;


/**
 * 监听Channel的事件，包括连接断开、连接建立
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface ChannelEventListener {
    public void onChannelConnect(final Channel channel);


    public void onChannelClose(final Channel channel);


    public void onChannelException(final Channel channel);
}
