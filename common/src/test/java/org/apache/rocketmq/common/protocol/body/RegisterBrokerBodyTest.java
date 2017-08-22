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

package org.apache.rocketmq.common.protocol.body;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.junit.Assert;
import org.junit.Test;

public class RegisterBrokerBodyTest {

    @Test
    public void encode() throws Exception {
        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
        List<String> filterServerList = new ArrayList<String>();
        for (int i = 0; i < 100; i++) {
            filterServerList.add("localhost:10911");
        }
        registerBrokerBody.setFilterServerList(filterServerList);

        TopicConfigSerializeWrapper wrapper = new TopicConfigSerializeWrapper();

        ConcurrentMap<String, TopicConfig> topicConfigs = wrapper.getTopicConfigTable();

        NumberFormat numberFormat = new DecimalFormat("0000000000");
        List<String> topics = new ArrayList<String>();
        for (int i = 0; i < 1024; i++) {
            TopicConfig topicConfig = new TopicConfig("Topic" + numberFormat.format(i));
            topicConfigs.put(topicConfig.getTopicName(), topicConfig);
            topics.add(topicConfig.getTopicName());
        }
        registerBrokerBody.setTopicConfigSerializeWrapper(wrapper);

        byte[] compressed = registerBrokerBody.encode(true);

        RegisterBrokerBody registerBrokerBodyBackUp = RegisterBrokerBody.decode(compressed, true);
        ConcurrentMap<String, TopicConfig> backupMap = registerBrokerBodyBackUp.getTopicConfigSerializeWrapper().getTopicConfigTable();

        List<String> backupTopics = new ArrayList<String>();
        for (ConcurrentMap.Entry<String, TopicConfig> next : backupMap.entrySet()) {
            backupTopics.add(next.getKey());
        }
        Collections.sort(topics);
        Collections.sort(backupTopics);
        Assert.assertEquals(topics, backupTopics);
    }
}