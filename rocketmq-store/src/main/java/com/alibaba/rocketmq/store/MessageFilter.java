/**
 * $Id: MessageFilter.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * 消息过滤接口
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface MessageFilter {
    public boolean isMessageMatched(final SubscriptionData subscriptionData, final long tagsCode);
}
