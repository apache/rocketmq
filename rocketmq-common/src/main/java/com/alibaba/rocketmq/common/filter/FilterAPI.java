package com.alibaba.rocketmq.common.filter;

import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-15
 */
public class FilterAPI {
    public static SubscriptionData buildSubscriptionData(String topic, String subString) {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);

        // TODO ×Ö¶Î×ª»¯
        subscriptionData.setHasAndOperator(false);

        return subscriptionData;
    }
}
