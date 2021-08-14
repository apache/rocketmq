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

package org.apache.rocketmq.client.producer.selector;


import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SelectMessageQueueSpecialLabel implements MessageQueueSelector {

    private double percentage = 0.2;
    private MessageQueueSelector defaultMessageQueueSelector;

    public SelectMessageQueueSpecialLabel() {
        this(new SelectMessageQueueByRandom());
    }

    public SelectMessageQueueSpecialLabel(double percentage) {
        this(percentage, new SelectMessageQueueByRandom());
    }

    public SelectMessageQueueSpecialLabel(MessageQueueSelector defaultMessageQueueSelector) {
        this.defaultMessageQueueSelector = defaultMessageQueueSelector;
    }

    public SelectMessageQueueSpecialLabel(double percentage, MessageQueueSelector defaultMessageQueueSelector) {
        this.percentage = percentage;
        this.defaultMessageQueueSelector = defaultMessageQueueSelector;
    }

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {

        Map<String, List<MessageQueue>> brokerQueuesTables = mqs.stream().collect(Collectors.toMap(mq -> mq.getBrokerName(), mq -> {
            List<MessageQueue> brokerQueues = new ArrayList<>();
            brokerQueues.add(mq);
            return brokerQueues;
        }, (brokerQueuesFirst, brokerQueuesSecond) -> {
            brokerQueuesFirst.addAll(brokerQueuesSecond);
            return brokerQueuesFirst;
        }));

        List<MessageQueue> specialLabelMessageQueueList = new ArrayList<>();

        brokerQueuesTables.forEach((brokerName, brokerQueueList) -> {
            int size = brokerQueueList.size();
            int queueSizePerBroker = (int) Math.ceil(size * percentage);
            Collections.sort(brokerQueueList);
            for (int i = 0; i < queueSizePerBroker; i++) {
                specialLabelMessageQueueList.add(brokerQueueList.get(i));
            }
        });
        List<MessageQueue> normalMqs = new ArrayList<>(mqs);
        specialLabelMessageQueueList.forEach(specialLabelMq -> normalMqs.remove(specialLabelMq));
        if(arg != null) {
            return defaultMessageQueueSelector.select(specialLabelMessageQueueList, msg, arg);
        } else {
            return defaultMessageQueueSelector.select(normalMqs, msg, arg);
        }

    }

}
