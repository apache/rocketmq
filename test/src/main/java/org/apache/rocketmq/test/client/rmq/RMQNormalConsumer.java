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

package org.apache.rocketmq.test.client.rmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.clientinterface.AbstractMQConsumer;
import org.apache.rocketmq.test.listener.AbstractListener;
import org.apache.rocketmq.test.util.RandomUtil;

public class RMQNormalConsumer extends AbstractMQConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RMQNormalConsumer.class);
    protected DefaultMQPushConsumer consumer = null;

    public RMQNormalConsumer(String nsAddr, String topic, String subExpression,
        String consumerGroup, AbstractListener listener) {
        super(nsAddr, topic, subExpression, consumerGroup, listener);
    }

    @Override
    public AbstractListener getListener() {
        return listener;
    }

    @Override
    public void setListener(AbstractListener listener) {
        this.listener = listener;
    }

    @Override
    public void create() {
        create(false);
    }

    @Override
    public void create(boolean useTLS) {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setInstanceName(RandomUtil.getStringByUUID());
        consumer.setNamesrvAddr(nsAddr);
        consumer.setPollNameServerInterval(100);
        try {
            consumer.subscribe(topic, subExpression);
        } catch (MQClientException e) {
            LOGGER.error("consumer subscribe failed!");
            e.printStackTrace();
        }
        consumer.setMessageListener(listener);
        consumer.setUseTLS(useTLS);
    }

    @Override
    public void start() {
        try {
            consumer.start();
            LOGGER.info(String.format("consumer[%s] started!", consumer.getConsumerGroup()));
        } catch (MQClientException e) {
            LOGGER.error("consumer start failed!");
            e.printStackTrace();
        }
    }

    public void subscribe(String topic, String subExpression) {
        try {
            consumer.subscribe(topic, subExpression);
        } catch (MQClientException e) {
            LOGGER.error("consumer subscribe failed!");
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {
        consumer.shutdown();
    }

    @Override
    public void clearMsg() {
        this.listener.clearMsg();
    }

    public void restart() {
        consumer.shutdown();
        create();
        start();
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }
}
