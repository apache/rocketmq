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

package org.apache.rocketmq.test.listener.rmq.order;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.test.listener.AbstractListener;

public class RMQOrderListener extends AbstractListener implements MessageListenerOrderly {
    private Map<String/* brokerId_brokerIp */, Collection<Object>> msgs = new ConcurrentHashMap<String, Collection<Object>>();

    public RMQOrderListener() {
        super();
    }

    public RMQOrderListener(String listnerName) {
        super(listnerName);
    }

    public RMQOrderListener(String originMsgCollector, String msgBodyCollector) {
        super(originMsgCollector, msgBodyCollector);
    }

    public Collection<Collection<Object>> getMsgs() {
        return msgs.values();
    }

    private void putMsg(MessageExt msg) {
        Collection<Object> msgQueue = null;
        String key = getKey(msg.getQueueId(), msg.getStoreHost().toString());
        if (!msgs.containsKey(key)) {
            msgQueue = new ArrayList<Object>();
        } else {
            msgQueue = msgs.get(key);
        }

        msgQueue.add(new String(msg.getBody()));
        msgs.put(key, msgQueue);
    }

    private String getKey(int queueId, String brokerIp) {
        return String.format("%s_%s", queueId, brokerIp);
    }

    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
        ConsumeOrderlyContext context) {
        for (MessageExt msg : msgs) {
            if (isDebug) {
                if (listnerName != null && listnerName != "") {
                    logger.info(listnerName + ": " + msg);
                } else {
                    logger.info(msg);
                }
            }

            putMsg(msg);
            msgBodys.addData(new String(msg.getBody()));
            originMsgs.addData(msg);
        }

        return ConsumeOrderlyStatus.SUCCESS;
    }
}
