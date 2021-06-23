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
package org.apache.rocketmq.common.fastjson;

import com.alibaba.fastjson.JSON;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.LogicalQueuesInfo;
import org.apache.rocketmq.common.protocol.route.LogicalQueuesInfoUnordered;
import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.assertj.core.util.Lists;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GenericMapSuperclassDeserializerTest {
    @Test
    public void testLogicalQueuesInfo() throws Exception {
        LogicalQueuesInfo logicalQueuesInfo = new LogicalQueuesInfo();
        logicalQueuesInfo.put(0, Lists.newArrayList(new LogicalQueueRouteData(1, 2, new MessageQueue("topic", "broker", 3), MessageQueueRouteState.Normal, 4, 5, 6, 7, "127.1.2.3")));

        byte[] buf = JSON.toJSONBytes(logicalQueuesInfo);

        LogicalQueuesInfo newLogicalQueuesInfo = JSON.parseObject(buf, LogicalQueuesInfo.class);

        assertThat(newLogicalQueuesInfo).isEqualTo(logicalQueuesInfo);
    }

    @Test
    public void testLogicalQueuesInfoUnordered() throws Exception {
        LogicalQueuesInfoUnordered logicalQueuesInfoUnordered = new LogicalQueuesInfoUnordered();
        MessageQueue mq = new MessageQueue("topic", "broker", 3);
        logicalQueuesInfoUnordered.put(0, new ConcurrentHashMap<LogicalQueuesInfoUnordered.Key, LogicalQueueRouteData>(Collections.singletonMap(new LogicalQueuesInfoUnordered.Key(mq.getBrokerName(), mq.getQueueId(), 4), new LogicalQueueRouteData(1, 2, mq, MessageQueueRouteState.Normal, 4, 5, 6, 7, "127.1.2.3"))));

        byte[] buf = JSON.toJSONBytes(logicalQueuesInfoUnordered);

        LogicalQueuesInfoUnordered newLogicalQueuesInfoUnordered = JSON.parseObject(buf, LogicalQueuesInfoUnordered.class);

        assertThat(newLogicalQueuesInfoUnordered).isEqualTo(logicalQueuesInfoUnordered);
    }
}