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

package org.apache.rocketmq.test.factory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.util.RandomUtil;

public class MQMessageFactory {
    private static Integer index = 0;

    public static List<Object> getRMQMessage(String tag, String topic, int msgSize) {
        List<Object> msgs = new ArrayList<Object>();
        for (int i = 0; i < msgSize; i++) {
            msgs.add(new Message(topic, tag, RandomUtil.getStringByUUID().getBytes(StandardCharsets.UTF_8)));
        }

        return msgs;
    }

    public static List<Object> getRMQMessage(List<String> tags, String topic, int msgSize) {
        List<Object> msgs = new ArrayList<Object>();
        for (int i = 0; i < msgSize; i++) {
            for (String tag : tags) {
                msgs.add(new Message(topic, tag, RandomUtil.getStringByUUID().getBytes(StandardCharsets.UTF_8)));
            }
        }
        return msgs;
    }

    public static List<Object> getMessageBody(List<Object> msgs) {
        List<Object> msgBodys = new ArrayList<Object>();
        for (Object msg : msgs) {
            msgBodys.add(new String(((Message) msg).getBody(), StandardCharsets.UTF_8));
        }

        return msgBodys;
    }

    public static Collection<Object> getMessage(Collection<Object>... msgs) {
        Collection<Object> allMsgs = new ArrayList<Object>();
        for (Collection<Object> msg : msgs) {
            allMsgs.addAll(msg);
        }
        return allMsgs;
    }

    public static List<Object> getDelayMsg(String topic, int delayLevel, int msgSize) {
        List<Object> msgs = new ArrayList<Object>();
        for (int i = 0; i < msgSize; i++) {
            Message msg = new Message(topic, RandomUtil.getStringByUUID().getBytes(StandardCharsets.UTF_8));
            msg.setDelayTimeLevel(delayLevel);
            msgs.add(msg);
        }
        return msgs;
    }

    public static List<Object> getKeyMsg(String topic, String key, int msgSize) {
        List<Object> msgs = new ArrayList<Object>();
        for (int i = 0; i < msgSize; i++) {
            Message msg = new Message(topic, null, key, RandomUtil.getStringByUUID().getBytes(StandardCharsets.UTF_8));
            msgs.add(msg);
        }
        return msgs;
    }

    public static Map<MessageQueue, List<Object>> getMsgByMQ(MessageQueue mq, int msgSize) {
        List<MessageQueue> mqs = new ArrayList<MessageQueue>();
        mqs.add(mq);
        return getMsgByMQ(mqs, msgSize);
    }

    public static Map<MessageQueue, List<Object>> getMsgByMQ(List<MessageQueue> mqs, int msgSize) {
        return getMsgByMQ(mqs, msgSize, null);
    }

    public static Map<MessageQueue, List<Object>> getMsgByMQ(List<MessageQueue> mqs, int msgSize,
        String tag) {
        Map<MessageQueue, List<Object>> msgs = new HashMap<MessageQueue, List<Object>>();
        for (MessageQueue mq : mqs) {
            msgs.put(mq, getMsg(mq.getTopic(), msgSize, tag));
        }
        return msgs;
    }

    public static List<Object> getMsg(String topic, int msgSize) {
        return getMsg(topic, msgSize, null);
    }

    public static List<Object> getMsg(String topic, int msgSize, String tag) {
        List<Object> msgs = new ArrayList<Object>();
        while (msgSize > 0) {
            Message msg = new Message(topic, (index++).toString().getBytes(StandardCharsets.UTF_8));
            if (tag != null) {
                msg.setTags(tag);
            }
            msgs.add(msg);
            msgSize--;
        }

        return msgs;
    }

    public static List<MessageQueue> getMessageQueues(MessageQueue... mq) {
        return Arrays.asList(mq);
    }
}
