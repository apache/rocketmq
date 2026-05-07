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

package org.apache.rocketmq.remoting.protocol.body;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConsumerOffsetSerializeWrapperTest {

    @Test
    public void testDecodeWithUnknownFields() {
        String consumerOffsetJson = "{" +
            "\"dataVersion\":{\"counter\":736456,\"stateVersion\":0,\"timestamp\":1778062479960}," +
            "\"groupTopicMap\":{" +
            "\"gid-test-rocketmq\":[\"TopicTest-high\",\"%RETRY%gid-test-rocketmq\"]" +
            "}," +
            "\"offsetTable\":{" +
            "\"TopicTest-high@gid-test\":{0:4954824657,1:4954593219,2:80629020}," +
            "\"TopicTest@gid\":{0:123}" +
            "}," +
            "\"pullOffsetTable\":{" +
            "\"TopicTest-high@gid-test\":{0:4953634646,1:4953403208}" +
            "}" +
            "}";

        ConsumerOffsetSerializeWrapper result = RemotingSerializable.decode(
            consumerOffsetJson.getBytes(StandardCharsets.UTF_8),
            ConsumerOffsetSerializeWrapper.class
        );

        assertNotNull(result);
        assertNotNull(result.getOffsetTable());
        assertNotNull(result.getDataVersion());

        Map<String, ConcurrentMap<Integer, Long>> offsetTable = result.getOffsetTable();
        assertNotNull(offsetTable.get("TopicTest-high@gid-test"));
        assertEquals(4954824657L, offsetTable.get("TopicTest-high@gid-test").get(0).longValue());
        assertEquals(4954593219L, offsetTable.get("TopicTest-high@gid-test").get(1).longValue());
    }
}