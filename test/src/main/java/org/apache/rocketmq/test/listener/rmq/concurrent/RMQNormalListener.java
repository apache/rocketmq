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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.test.listener.AbstractListener;

public class RMQNormalListener extends AbstractListener implements MessageListenerConcurrently {
    private ConsumeConcurrentlyStatus consumeStatus = ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    private AtomicInteger msgIndex = new AtomicInteger(0);

    public RMQNormalListener() {
        super();
    }

    public RMQNormalListener(String listenerName) {
        super(listenerName);
    }

    public RMQNormalListener(ConsumeConcurrentlyStatus consumeStatus) {
        super();
        this.consumeStatus = consumeStatus;
    }

    public RMQNormalListener(String originMsgCollector, String msgBodyCollector) {
        super(originMsgCollector, msgBodyCollector);
    }

    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
        ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        for (MessageExt msg : msgs) {
            msgIndex.getAndIncrement();
            if (isDebug) {
                if (listenerName != null && !listenerName.isEmpty()) {
                    logger.info(listenerName + ":" + msgIndex.get() + ":"
                        + String.format("msgid:%s broker:%s queueId:%s offset:%s",
                        msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(),
                        msg.getQueueOffset()));
                } else {
                    logger.info(msg);
                }
            }

            msgBodys.addData(new String(msg.getBody()));
            originMsgs.addData(msg);
            if (originMsgIndex != null) {
                originMsgIndex.put(new String(msg.getBody()), msg);
            }
        }
        return consumeStatus;
    }
}
