/**
 * $Id$
 */
package com.alibaba.rocketmq.broker.client;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import io.netty.channel.Channel;


/**
 * 管理Producer组及各个Producer连接
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class ProducerManager {
    private final ConcurrentHashMap<String, Set<Channel>> groupHashcodeChannelTable =
            new ConcurrentHashMap<String, Set<Channel>>();


    public ProducerManager() {

    }


    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {

    }
}
