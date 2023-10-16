/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.client.consumer.pop;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQPopClient;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

@Ignore
public class BasePopNormally extends BasePop {

    protected String topic;
    protected String group;
    protected RMQNormalProducer producer = null;
    protected RMQPopClient client = null;
    protected String brokerAddr;
    protected MessageQueue messageQueue;

    @Before
    public void setUp() {
        brokerAddr = brokerController1.getBrokerAddr();
        topic = MQRandomUtils.getRandomTopic();
        group = initConsumerGroup();
        IntegrationTestBase.initTopic(topic, NAMESRV_ADDR, BROKER1_NAME, 8, CQType.SimpleCQ, TopicMessageType.NORMAL);
        producer = getProducer(NAMESRV_ADDR, topic);
        client = getRMQPopClient();
        messageQueue = new MessageQueue(topic, BROKER1_NAME, -1);
    }

    @After
    public void tearDown() {
        shutdown();
    }

    protected CompletableFuture<PopResult> popMessageAsync(long invisibleTime, int maxNums, long timeout) {
        return client.popMessageAsync(
            brokerAddr, messageQueue, invisibleTime, maxNums, group, timeout, true,
            ConsumeInitMode.MIN, false, ExpressionType.TAG, "*");
    }

    protected CompletableFuture<PopResult> popMessageAsync(long invisibleTime, int maxNums) {
        return client.popMessageAsync(
            brokerAddr, messageQueue, invisibleTime, maxNums, group, 3000, false,
            ConsumeInitMode.MIN, false, ExpressionType.TAG, "*");
    }
}
