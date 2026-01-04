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
package org.apache.rocketmq.remoting.protocol.header;

import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtraInfoUtilTest {

    @Test
    public void testOrderCountInfo() {
        String topic = "TOPIC";
        int queueId = 0;
        long queueOffset = 1234;

        Integer queueIdCount = 1;
        Integer queueOffsetCount = 2;

        String queueIdKey = ExtraInfoUtil.getStartOffsetInfoMapKey(topic, queueId);
        String queueOffsetKey = ExtraInfoUtil.getQueueOffsetMapKey(topic, queueId, queueOffset);

        StringBuilder sb = new StringBuilder();
        ExtraInfoUtil.buildQueueIdOrderCountInfo(sb, topic, queueId, queueIdCount);
        ExtraInfoUtil.buildQueueOffsetOrderCountInfo(sb, topic, queueId, queueOffset, queueOffsetCount);
        Map<String, Integer> orderCountInfo = ExtraInfoUtil.parseOrderCountInfo(sb.toString());

        assertEquals(queueIdCount, orderCountInfo.get(queueIdKey));
        assertEquals(queueOffsetCount, orderCountInfo.get(queueOffsetKey));
    }
}
