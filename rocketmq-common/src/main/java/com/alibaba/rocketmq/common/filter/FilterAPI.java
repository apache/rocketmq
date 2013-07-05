package com.alibaba.rocketmq.common.filter;

import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-15
 */
public class FilterAPI {
    public static SubscriptionData buildSubscriptionData(String topic, String subString) throws Exception {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);

        if (null == subString || subString.equals(SubscriptionData.SUB_ALL)) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
        }
        else {
            String[] tags = subString.split("\\|\\|");
            if (tags != null && tags.length > 0) {
                for (String tag : tags) {
                    if (tag.length() > 0) {
                        String trimString = tag.trim();
                        subscriptionData.getTagsSet().add(trimString);
                        subscriptionData.getCodeSet().add(trimString.hashCode());
                    }
                }
            }
            else {
                throw new Exception("subString split error");
            }
        }

        return subscriptionData;
    }

}
