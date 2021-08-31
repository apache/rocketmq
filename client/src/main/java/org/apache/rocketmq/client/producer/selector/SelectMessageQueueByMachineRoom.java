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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

public class SelectMessageQueueByMachineRoom implements MessageQueueSelector {
    private Random random = new Random(System.currentTimeMillis());
    private Set<String> consumeridcs;

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        List<MessageQueue> preMqAll = new ArrayList<MessageQueue>();
        for (MessageQueue mq : mqs) {
            String[] temp = mq.getBrokerName().split("@");
            if (temp.length == 2 && consumeridcs.contains(temp[0])) {
                preMqAll.add(mq);
            }
        }

        if (preMqAll.size() == 0) {
            preMqAll = mqs;
        }

        if (arg == null) {
            int value = random.nextInt(preMqAll.size());
            return preMqAll.get(value);
        }

        int value = arg.hashCode() % preMqAll.size();
        if (value < 0) {
            value = Math.abs(value);
        }
        return preMqAll.get(value);
    }

    public Set<String> getConsumeridcs() {
        return consumeridcs;
    }

    public void setConsumeridcs(Set<String> consumeridcs) {
        this.consumeridcs = consumeridcs;
    }
}
