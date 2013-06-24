package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.List;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-24
 */
public interface ConsumerIdsChangeListener {
    public void consumerIdsChanged(final String group, final List<Channel> channels);
}
