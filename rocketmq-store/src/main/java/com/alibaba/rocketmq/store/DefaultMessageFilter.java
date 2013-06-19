/**
 * $Id: DefaultMessageFilter.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * 消息过滤规则实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class DefaultMessageFilter implements MessageFilter {

    @Override
    public boolean isMessageMatched(SubscriptionData subscriptionData, long tagsCode) {
        if (null == subscriptionData) {
            return true;
        }

        if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
            return true;
        }

        return subscriptionData.getCodeSet().contains((int) tagsCode);
    }

}
