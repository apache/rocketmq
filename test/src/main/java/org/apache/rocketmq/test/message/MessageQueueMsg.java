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

package org.apache.rocketmq.test.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.factory.MQMessageFactory;

public class MessageQueueMsg {
    private Map<MessageQueue, List<Object>> msgsWithMQ = null;
    private Map<Integer, List<Object>> msgsWithMQId = null;
    private Collection<Object> msgBodys = null;

    public MessageQueueMsg(List<MessageQueue> mqs, int msgSize) {
        this(mqs, msgSize, null);
    }

    public MessageQueueMsg(List<MessageQueue> mqs, int msgSize, String tag) {
        msgsWithMQ = MQMessageFactory.getMsgByMQ(mqs, msgSize, tag);
        msgsWithMQId = new HashMap<Integer, List<Object>>();
        msgBodys = new ArrayList<Object>();
        init();
    }

    public Map<MessageQueue, List<Object>> getMsgsWithMQ() {
        return msgsWithMQ;
    }

    public Map<Integer, List<Object>> getMsgWithMQId() {
        return msgsWithMQId;
    }

    public Collection<Object> getMsgBodys() {
        return msgBodys;
    }

    private void init() {
        for (MessageQueue mq : msgsWithMQ.keySet()) {
            msgsWithMQId.put(mq.getQueueId(), msgsWithMQ.get(mq));
            msgBodys.addAll(MQMessageFactory.getMessageBody(msgsWithMQ.get(mq)));
        }
    }
}
