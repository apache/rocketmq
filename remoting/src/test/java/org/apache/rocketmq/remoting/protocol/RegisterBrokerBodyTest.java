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

package org.apache.rocketmq.remoting.protocol;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RegisterBrokerBodyTest {
    @Test
    public void test_encode_decode() throws IOException {
        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
        TopicConfigAndMappingSerializeWrapper topicConfigSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();
        registerBrokerBody.setTopicConfigSerializeWrapper(topicConfigSerializeWrapper);

        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        for (int i = 0; i < 10000; i++) {
            topicConfigTable.put(String.valueOf(i), new TopicConfig(String.valueOf(i)));
        }

        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        byte[] compareEncode = registerBrokerBody.encode(true);
        byte[] encode2 = registerBrokerBody.encode(false);
        RegisterBrokerBody decodeRegisterBrokerBody = RegisterBrokerBody.decode(compareEncode, true, MQVersion.Version.V5_0_0);

        assertEquals(registerBrokerBody.getTopicConfigSerializeWrapper().getTopicConfigTable().size(), decodeRegisterBrokerBody.getTopicConfigSerializeWrapper().getTopicConfigTable().size());

    }
}
