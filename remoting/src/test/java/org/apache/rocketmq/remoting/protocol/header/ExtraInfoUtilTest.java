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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.KeyBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

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

    @Test
    public void testStartOffsetInfoRoundTrip() {
        String retryTopic = KeyBuilder.buildPopRetryTopicV2("TOPIC", "GROUP");
        StringBuilder sb = new StringBuilder();
        ExtraInfoUtil.buildStartOffsetInfo(sb, "TOPIC", 0, 100L);
        ExtraInfoUtil.buildStartOffsetInfo(sb, retryTopic, 1, 200L);

        Map<String, Long> map = ExtraInfoUtil.parseStartOffsetInfo(sb.toString());
        assertEquals(Long.valueOf(100L), map.get(ExtraInfoUtil.getStartOffsetInfoMapKey("TOPIC", 0)));
        assertEquals(Long.valueOf(200L), map.get(ExtraInfoUtil.getStartOffsetInfoMapKey(retryTopic, 1)));
    }

    @Test
    public void testMsgOffsetInfoRoundTrip() {
        StringBuilder sb = new StringBuilder();
        ExtraInfoUtil.buildMsgOffsetInfo(sb, "TOPIC", 0, Arrays.asList(1L, 2L, 3L));
        ExtraInfoUtil.buildMsgOffsetInfo(sb, "TOPIC", 1, Collections.singletonList(9L));

        Map<String, List<Long>> map = ExtraInfoUtil.parseMsgOffsetInfo(sb.toString());
        assertEquals(Arrays.asList(1L, 2L, 3L), map.get(ExtraInfoUtil.getStartOffsetInfoMapKey("TOPIC", 0)));
        assertEquals(Collections.singletonList(9L), map.get(ExtraInfoUtil.getStartOffsetInfoMapKey("TOPIC", 1)));
    }

    @Test
    public void testExtraInfoGetters() {
        String extraInfo = ExtraInfoUtil.buildExtraInfo(100L, 1690000000000L, 60000L, 3, "TOPIC", "broker-a", 2, 101L);
        String[] parts = ExtraInfoUtil.split(extraInfo);

        assertEquals(100L, ExtraInfoUtil.getCkQueueOffset(parts));
        assertEquals(1690000000000L, ExtraInfoUtil.getPopTime(parts));
        assertEquals(60000L, ExtraInfoUtil.getInvisibleTime(parts));
        assertEquals(3, ExtraInfoUtil.getReviveQid(parts));
        assertEquals("broker-a", ExtraInfoUtil.getBrokerName(parts));
        assertEquals(2, ExtraInfoUtil.getQueueId(parts));
        assertEquals(101L, ExtraInfoUtil.getQueueOffset(parts));
    }

    @Test
    public void testParseMalformedEntriesThrow() {
        for (String bad : new String[] {"0 1", "0 1 2 3", " 0 1", "0  1", "0 1 "}) {
            assertThrows(IllegalArgumentException.class, () -> ExtraInfoUtil.parseStartOffsetInfo(bad));
            assertThrows(IllegalArgumentException.class, () -> ExtraInfoUtil.parseOrderCountInfo(bad));
            assertThrows(IllegalArgumentException.class, () -> ExtraInfoUtil.parseMsgOffsetInfo(bad));
        }
    }
}
