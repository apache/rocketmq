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
package org.apache.rocketmq.test.client.consumer.topic;

import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.Test;

import java.util.Set;

public class TopicListTest {

    public static boolean checkEqual(Set<String> lh, Set<String> rh) {
        if (lh.size() != rh.size()) {
            return false;
        }
        if (!rh.containsAll(lh)) {
            return false;
        }
        return true;
    }

    @Test
    public void topicTestFromJson() {
        TopicList topicList = new TopicList();
        topicList.setBrokerAddr(RandomUtils.getStringByUUID());
        int num= RandomUtils.getIntegerBetween(0,20);
        for (int i = 0; i < num; i++) {
            topicList.getTopicList().add(RandomUtils.getStringByUUID());
        }
        String json = RemotingSerializable.toJson(topicList, true);
        TopicList fromJson = RemotingSerializable.fromJson(json, TopicList.class);
        assert (checkEqual(topicList.getTopicList(), fromJson.getTopicList()));
        assert (fromJson.getBrokerAddr().equals(topicList.getBrokerAddr()));
    }
}
