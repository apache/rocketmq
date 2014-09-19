package com.alibaba.rocketmq.common.filter;

import org.junit.Test;

import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-18
 */
public class FilterAPITest {

    @Test
    public void testBuildSubscriptionData() throws Exception {
        SubscriptionData subscriptionData =
                FilterAPI.buildSubscriptionData("ConsumerGroup1", "TestTopic", "TAG1 || Tag2 || tag3");
        System.out.println(subscriptionData);
    }
}
