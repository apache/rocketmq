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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TagMessage {
    private List<String> tags = null;
    private String topic = null;
    private int msgSize = 0;
    private Map<String, List<Object>> rmqMsgs = new HashMap<String, List<Object>>();

    public TagMessage(String tag, String topic, int msgSize) {
        String[] tags = {tag};
        this.tags = Arrays.asList(tags);
        this.topic = topic;
        this.msgSize = msgSize;

        init();
    }

    public TagMessage(String[] tags, String topic, int msgSize) {
        this(Arrays.asList(tags), topic, msgSize);
    }

    public TagMessage(List<String> tags, String topic, int msgSize) {
        this.tags = tags;
        this.topic = topic;
        this.msgSize = msgSize;

        init();
    }

    private void init() {
        for (String tag : tags) {
            List<Object> tagMsgs = MQMessageFactory.getRMQMessage(tag, topic, msgSize);
            rmqMsgs.put(tag, tagMsgs);
        }
    }

    public List<Object> getMessageByTag(String tag) {
        if (tags.contains(tag)) {
            return rmqMsgs.get(tag);
        } else {
            return new ArrayList<Object>();
        }
    }

    public List<Object> getMixedTagMessages() {
        List<Object> mixedMsgs = new ArrayList<Object>();
        for (int i = 0; i < msgSize; i++) {
            for (String tag : tags) {
                mixedMsgs.add(rmqMsgs.get(tag).get(i));
            }
        }

        return mixedMsgs;
    }

    public List<Object> getMessageBodyByTag(String tag) {
        if (tags.contains(tag)) {
            return MQMessageFactory.getMessageBody(rmqMsgs.get(tag));
        } else {
            return new ArrayList<Object>();
        }
    }

    public List<Object> getMessageBodyByTag(String... tag) {
        return this.getMessageBodyByTag(Arrays.asList(tag));
    }

    public List<Object> getMessageBodyByTag(List<String> tags) {
        List<Object> msgBodys = new ArrayList<Object>();
        for (String tag : tags) {
            msgBodys.addAll(MQMessageFactory.getMessageBody(rmqMsgs.get(tag)));
        }
        return msgBodys;
    }

    public List<Object> getAllTagMessageBody() {
        List<Object> msgs = new ArrayList<Object>();
        for (String tag : tags) {
            msgs.addAll(MQMessageFactory.getMessageBody(rmqMsgs.get(tag)));
        }

        return msgs;
    }

}
