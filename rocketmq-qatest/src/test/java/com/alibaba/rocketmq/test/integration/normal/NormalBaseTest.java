package com.alibaba.rocketmq.test.integration.normal;

import com.alibaba.rocketmq.test.integration.BaseTest;


/**
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-26
 */
public class NormalBaseTest extends BaseTest {
    protected String consumerGroup = "qatest_consumer";
    protected String producerGroup = "qatest_producer";
    protected String topic = "qatest_TopicTest";
}
