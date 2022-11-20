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

package org.apache.rocketmq.test.client.consumer.broadcast;

import org.apache.log4j.Logger;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQBroadCastConsumer;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.AbstractListener;

public class BaseBroadcast extends BaseConf {
    private static Logger logger = Logger.getLogger(BaseBroadcast.class);

    public static RMQBroadCastConsumer getBroadCastConsumer(String nsAddr, String topic,
        String subExpression,
        AbstractListener listener) {
        String consumerGroup = initConsumerGroup();
        return getBroadCastConsumer(nsAddr, consumerGroup, topic, subExpression, listener);
    }

    public static RMQBroadCastConsumer getBroadCastConsumer(String nsAddr, String consumerGroup,
        String topic, String subExpression,
        AbstractListener listener) {
        RMQBroadCastConsumer consumer = ConsumerFactory.getRMQBroadCastConsumer(nsAddr,
            consumerGroup, topic, subExpression, listener);

        consumer.setDebug();

        mqClients.add(consumer);
        logger.info(String.format("consumer[%s] start,topic[%s],subExpression[%s]", consumerGroup,
            topic, subExpression));
        return consumer;
    }

    public void printSeparator() {
        for (int i = 0; i < 3; i++) {
            logger.info(
                "<<<<<<<<================================================================================>>>>>>>>");
        }
    }
}
