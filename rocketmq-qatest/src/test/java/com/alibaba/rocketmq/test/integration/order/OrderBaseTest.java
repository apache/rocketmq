package com.alibaba.rocketmq.test.integration.order;

import com.alibaba.rocketmq.test.integration.BaseTest;


/**
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-26
 */
public class OrderBaseTest extends BaseTest {
    protected String consumerGroup = "qatest_consumer_order_test";
    protected String producerGroup = "qatest_producer_order";
    protected String topic = "qatest_TopicTest3_virtual_order2";
}
