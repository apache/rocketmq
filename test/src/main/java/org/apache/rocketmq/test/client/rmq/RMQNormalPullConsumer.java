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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.test.client.rmq;

import org.apache.log4j.Logger;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.test.clientinterface.MQConsumer;
import org.apache.rocketmq.test.util.RandomUtil;

public class RMQNormalPullConsumer implements MQConsumer {

    private static Logger logger = Logger.getLogger(RMQNormalPullConsumer.class);
    protected DefaultMQPullConsumer consumer = null;
    private String nsAddr;
    private String consumerGroup;

    public RMQNormalPullConsumer(String nsAddr, String consumerGroup) {
        this.nsAddr = nsAddr;
        this.consumerGroup = consumerGroup;
        create();
        start();
    }

    @Override
    public void create() {
        create(false);
    }

    @Override
    public void create(boolean useTLS) {
        consumer = new DefaultMQPullConsumer(consumerGroup);
        consumer.setInstanceName(RandomUtil.getStringByUUID());
        consumer.setNamesrvAddr(nsAddr);
        consumer.setUseTLS(useTLS);
    }

    @Override
    public void start() {
        try {
            consumer.start();
            logger.info(String.format("consumer[%s] started!", consumer.getConsumerGroup()));
        } catch (MQClientException e) {
            logger.error("consumer start failed!");
        }
    }

    @Override
    public void shutdown() {
        consumer.shutdown();
    }

    public DefaultMQPullConsumer getConsumer() {
        return consumer;
    }
}
