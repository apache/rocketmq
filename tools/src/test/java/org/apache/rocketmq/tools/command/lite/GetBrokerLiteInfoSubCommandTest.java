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

package org.apache.rocketmq.tools.command.lite;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.remoting.protocol.body.GetBrokerLiteInfoResponseBody;
import org.junit.Test;

public class GetBrokerLiteInfoSubCommandTest {

    private GetBrokerLiteInfoResponseBody mockResponseBody() {
        GetBrokerLiteInfoResponseBody responseBody = new GetBrokerLiteInfoResponseBody();
        responseBody.setStoreType("RocksDB");
        responseBody.setMaxLmqNum(1000);
        responseBody.setCurrentLmqNum(500);
        responseBody.setLiteSubscriptionCount(200);

        // Mock topic meta data
        Map<String, Integer> topicMeta = new HashMap<>();
        topicMeta.put("TopicA", 10);
        topicMeta.put("TopicB", 20);
        responseBody.setTopicMeta(topicMeta);

        // Mock group meta data
        Map<String, Set<String>> groupMeta = new HashMap<>();
        Set<String> topics1 = new HashSet<>(Arrays.asList("TopicA", "TopicB"));
        Set<String> topics2 = new HashSet<>(Collections.singletonList("TopicC"));
        groupMeta.put("Group1", topics1);
        groupMeta.put("Group2", topics2);
        responseBody.setGroupMeta(groupMeta);

        return responseBody;
    }

    @Test
    public void testPrint() {
        GetBrokerLiteInfoResponseBody responseBody = mockResponseBody();
        GetBrokerLiteInfoSubCommand.printHeader();
        GetBrokerLiteInfoSubCommand.printRow(responseBody, "127.0.0.1:10911", true);
        GetBrokerLiteInfoSubCommand.printRow(responseBody, "127.0.0.1:10911", true);
    }

}
