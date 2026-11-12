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
package org.apache.rocketmq.store.pop;

import com.alibaba.fastjson2.JSON;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.junit.Assert;
import org.junit.Test;

public class PopCheckPointTest {

    @Test
    public void testToJsonBytesMatchesJsonStringBytes() {
        PopCheckPoint ck = new PopCheckPoint();
        ck.setTopic("topic-\u4e2d\u6587");
        ck.setCId("group");
        ck.setQueueId(3);
        ck.setStartOffset(200L);
        ck.setPopTime(1670212915531L);
        ck.setInvisibleTime(60000L);
        ck.setBitMap(5);
        ck.setNum((byte) 2);
        ck.setBrokerName("broker-a");
        ck.addDiff(1);
        ck.addDiff(3);
        ck.setRePutTimes("1");

        byte[] direct = JSON.toJSONBytes(ck);
        Assert.assertArrayEquals(JSON.toJSONString(ck).getBytes(StandardCharsets.UTF_8), direct);

        PopCheckPoint decoded = JSON.parseObject(direct, PopCheckPoint.class);
        Assert.assertEquals(ck.getTopic(), decoded.getTopic());
        Assert.assertEquals(ck.getCId(), decoded.getCId());
        Assert.assertEquals(ck.getStartOffset(), decoded.getStartOffset());
        Assert.assertEquals(ck.getPopTime(), decoded.getPopTime());
        Assert.assertEquals(ck.getQueueOffsetDiff(), decoded.getQueueOffsetDiff());
        Assert.assertEquals(ck.getBitMap(), decoded.getBitMap());
    }

    private static final long OVER_INT_OFFSET = (long) Integer.MAX_VALUE + 1L;

    @Test
    public void testCompareToDoesNotTruncateLargeStartOffsets() {
        PopCheckPoint zero = buildCheckPoint("topic-a", "group-a", 0, 0L, 1L, "broker-a", 0L);
        PopCheckPoint overInt = buildCheckPoint("topic-a", "group-a", 0, 3_000_000_000L, 1L, "broker-a", 0L);

        // A subtraction-based comparator casts the difference to int, so 3_000_000_000L - 0L wraps
        // to a negative value and both directions would report the same order. The full 64-bit
        // comparison must keep the two directions strictly opposite.
        Assert.assertTrue(zero.compareTo(overInt) < 0);
        Assert.assertTrue(overInt.compareTo(zero) > 0);
        Assert.assertEquals(-Integer.signum(overInt.compareTo(zero)), Integer.signum(zero.compareTo(overInt)));

        PopCheckPoint maxOffset = buildCheckPoint("topic-a", "group-a", 0, Long.MAX_VALUE, 1L, "broker-a", 0L);
        Assert.assertTrue(overInt.compareTo(maxOffset) < 0);
        Assert.assertTrue(maxOffset.compareTo(overInt) > 0);
        // Antisymmetry: swapping the operands must flip the sign, never keep it.
        Assert.assertEquals(-Integer.signum(maxOffset.compareTo(overInt)), Integer.signum(overInt.compareTo(maxOffset)));
    }

    @Test
    public void testSortOrdersByStartOffsetAcrossMixedOriginalTopics() {
        List<PopCheckPoint> checkPoints = new ArrayList<>();
        checkPoints.add(buildCheckPoint("topic-b", "group-b", 3, 5_000_000_000L, 1L, "broker-b", 3L));
        checkPoints.add(buildCheckPoint("topic-a", "group-a", 1, 0L, 1L, "broker-a", 0L));
        checkPoints.add(buildCheckPoint("topic-c", "group-c", 2, OVER_INT_OFFSET, 1L, "broker-c", 1L));
        checkPoints.add(buildCheckPoint("topic-a", "group-a", 1, 2L, 1L, "broker-a", 2L));

        Collections.sort(checkPoints);

        // Every adjacent pair must be ordered ascending; a truncated comparator would leave the
        // over-int entry in the wrong place and could even violate the ordering contract.
        for (int i = 0; i < checkPoints.size() - 1; i++) {
            Assert.assertTrue(checkPoints.get(i).getStartOffset() <= checkPoints.get(i + 1).getStartOffset());
            Assert.assertTrue(checkPoints.get(i).compareTo(checkPoints.get(i + 1)) <= 0);
        }
        Assert.assertEquals(0L, checkPoints.get(0).getStartOffset());
        Assert.assertEquals(2L, checkPoints.get(1).getStartOffset());
        Assert.assertEquals(OVER_INT_OFFSET, checkPoints.get(2).getStartOffset());
        Assert.assertEquals(5_000_000_000L, checkPoints.get(3).getStartOffset());
    }

    @Test
    public void testTreeMapNavigationWithLargeStartOffsets() {
        NavigableMap<PopCheckPoint, String> map = new TreeMap<>();
        PopCheckPoint zero = buildCheckPoint("topic-a", "group-a", 0, 0L, 1L, "broker-a", 0L);
        PopCheckPoint overInt = buildCheckPoint("topic-a", "group-a", 0, 3_000_000_000L, 2L, "broker-a", 1L);
        PopCheckPoint maxOffset = buildCheckPoint("topic-a", "group-a", 0, Long.MAX_VALUE, 3L, "broker-a", 2L);
        map.put(zero, "zero");
        map.put(overInt, "over-int");
        map.put(maxOffset, "max");

        Assert.assertEquals(3, map.size());
        // containsKey/get navigate the tree with compareTo, so a truncating comparator would look
        // in the wrong subtree and report a present key as missing.
        Assert.assertTrue(map.containsKey(overInt));
        Assert.assertTrue(map.containsKey(maxOffset));
        Assert.assertEquals("over-int", map.get(overInt));
        Assert.assertEquals(zero, map.firstKey());

        Assert.assertEquals("zero", map.remove(zero));
        Assert.assertEquals(2, map.size());
        Assert.assertFalse(map.containsKey(zero));
        Assert.assertEquals(overInt, map.firstKey());
        Assert.assertEquals("max", map.remove(maxOffset));
        Assert.assertEquals(1, map.size());
    }

    @Test
    public void testEqualStartOffsetWithDifferentTopicOrGroupAreDistinctKeys() {
        PopCheckPoint fromTopicA = buildCheckPoint("topic-a", "group-a", 0, 100L, 1L, "broker-a", 1L);
        PopCheckPoint fromTopicB = buildCheckPoint("topic-b", "group-b", 0, 100L, 1L, "broker-a", 2L);
        PopCheckPoint sameTopicOtherGroup = buildCheckPoint("topic-a", "group-b", 0, 100L, 1L, "broker-a", 3L);

        Assert.assertNotEquals(fromTopicA, fromTopicB);
        Assert.assertNotEquals(0, fromTopicA.compareTo(fromTopicB));
        Assert.assertNotEquals(0, fromTopicA.compareTo(sameTopicOtherGroup));

        NavigableMap<PopCheckPoint, String> map = new TreeMap<>();
        map.put(fromTopicA, "topic-a");
        map.put(fromTopicB, "topic-b");
        map.put(sameTopicOtherGroup, "topic-a-other-group");

        // All three checkpoints must survive; startOffset-only ordering collapsed them into one key
        // and silently dropped the completion state of the others.
        Assert.assertEquals(3, map.size());
        Assert.assertEquals("topic-a", map.get(fromTopicA));
        Assert.assertEquals("topic-b", map.get(fromTopicB));
        Assert.assertEquals("topic-a-other-group", map.get(sameTopicOtherGroup));
    }

    @Test
    public void testEqualsAndHashCodeFollowCompareTo() {
        PopCheckPoint base = buildCheckPoint("topic-a", "group-a", 1, 100L, 10L, "broker-a", 5L);
        PopCheckPoint same = buildCheckPoint("topic-a", "group-a", 1, 100L, 10L, "broker-a", 5L);
        Assert.assertEquals(base, same);
        Assert.assertEquals(base.hashCode(), same.hashCode());
        Assert.assertEquals(0, base.compareTo(same));

        // A different popTime is a different checkpoint even with the same startOffset, which is why
        // the revive flow includes it in its merge key and the ordering must not treat them as equal.
        PopCheckPoint otherPopTime = buildCheckPoint("topic-a", "group-a", 1, 100L, 11L, "broker-a", 5L);
        Assert.assertNotEquals(base, otherPopTime);
        Assert.assertNotEquals(0, base.compareTo(otherPopTime));
    }

    @Test
    public void testCompareToToleratesNullTopicAndBrokerName() {
        PopCheckPoint noBroker = new PopCheckPoint();
        noBroker.setStartOffset(100L);
        noBroker.setPopTime(1L);
        noBroker.setQueueId(0);
        noBroker.setCId("group");
        noBroker.setReviveOffset(1L);

        PopCheckPoint withBroker = buildCheckPoint(null, "group", 0, 100L, 1L, "broker-a", 1L);

        // A mock checkpoint built for a late ack may carry a null brokerName; comparing it must not
        // throw and must stay antisymmetric.
        Assert.assertTrue(noBroker.compareTo(withBroker) < 0);
        Assert.assertTrue(withBroker.compareTo(noBroker) > 0);
    }

    private static PopCheckPoint buildCheckPoint(String topic, String cid, int queueId, long startOffset,
        long popTime, String brokerName, long reviveOffset) {
        PopCheckPoint ck = new PopCheckPoint();
        ck.setTopic(topic);
        ck.setCId(cid);
        ck.setQueueId(queueId);
        ck.setStartOffset(startOffset);
        ck.setPopTime(popTime);
        ck.setBrokerName(brokerName);
        ck.setReviveOffset(reviveOffset);
        return ck;
    }
}
