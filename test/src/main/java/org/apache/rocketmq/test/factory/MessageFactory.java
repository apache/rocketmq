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
import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.test.util.RandomUtils;

public class MessageFactory {

    public static Message getRandomMessage(String topic) {
        return getStringMessage(topic, RandomUtils.getStringByUUID());
    }

    public static Message getStringMessage(String topic, String body) {
        return new Message(topic, body.getBytes(StandardCharsets.UTF_8));
    }

    public static Message getStringMessageByTag(String topic, String tags, String body) {
        return new Message(topic, tags, body.getBytes(StandardCharsets.UTF_8));
    }

    public static Message getRandomMessageByTag(String topic, String tags) {
        return getStringMessageByTag(topic, tags, RandomUtils.getStringByUUID());
    }

    public static Collection<Message> getRandomMessageList(String topic, int size) {
        List<Message> msgList = new ArrayList<Message>();
        for (int i = 0; i < size; i++) {
            msgList.add(getRandomMessage(topic));
        }
        return msgList;
    }

    public static Collection<Message> getRandomMessageListByTag(String topic, String tags, int size) {
        List<Message> msgList = new ArrayList<Message>();
        for (int i = 0; i < size; i++) {
            msgList.add(getRandomMessageByTag(topic, tags));
        }
        return msgList;
    }

}
