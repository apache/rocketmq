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

package org.apache.rocketmq.test.listener.rmq.concurrent;

import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.test.listener.AbstractListener;
import org.apache.rocketmq.test.util.RandomUtil;
import org.apache.rocketmq.test.util.data.collect.DataCollector;
import org.apache.rocketmq.test.util.data.collect.DataCollectorManager;

import java.util.Collection;
import java.util.List;

public class RMQDelayListener extends AbstractListener implements MessageListenerConcurrently {
    private DataCollector msgDelayTimes = null;

    public RMQDelayListener() {
        msgDelayTimes = DataCollectorManager.getInstance()
            .fetchDataCollector(RandomUtil.getStringByUUID());
    }

    public Collection<Object> getMsgDelayTimes() {
        return msgDelayTimes.getAllData();
    }

    public void resetMsgDelayTimes() {
        msgDelayTimes.resetData();
    }

    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
        ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        long recvTime = System.currentTimeMillis();
        for (MessageExt msg : msgs) {
            if (isDebug) {
                LOGGER.info(listenerName + ":" + msg);
            }

            msgBodys.addData(new String(msg.getBody(), StandardCharsets.UTF_8));
            originMsgs.addData(msg);
            msgDelayTimes.addData(Math.abs(recvTime - msg.getBornTimestamp()));
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
