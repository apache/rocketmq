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

package org.apache.rocketmq.common;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.assertj.core.util.Lists;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class RegisterBrokerBodyTest {
    @Test
    public void test_encode_decode() throws IOException {
        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        registerBrokerBody.setTopicConfigSerializeWrapper(topicConfigSerializeWrapper);
        registerBrokerBody.setFilterServerList(Lists.newArrayList("127.0.0.1"));

        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
        for (int i = 0; i < 2000; i++) {
            topicConfigTable.put(String.valueOf(i), new TopicConfig(String.valueOf(i)));
        }

        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);
        final byte[] sourceByte = registerBrokerBody.encode();
        final byte[] gzipEncode = registerBrokerBody.gzipEncode(sourceByte);

        System.out.println(sourceByte.length);
        System.out.println(gzipEncode.length);
        RegisterBrokerBody decodeRegisterBrokerBody = RegisterBrokerBody.decode(gzipEncode, sourceByte.length);

        assertEquals(registerBrokerBody.getTopicConfigSerializeWrapper().getTopicConfigTable().size(), decodeRegisterBrokerBody.getTopicConfigSerializeWrapper().getTopicConfigTable().size());

    }
}
