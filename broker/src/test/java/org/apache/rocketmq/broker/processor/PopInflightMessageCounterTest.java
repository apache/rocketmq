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
package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PopInflightMessageCounterTest {

    @Test
    public void testNum() {
        BrokerController brokerController = mock(BrokerController.class);
        long brokerStartTime = System.currentTimeMillis();
        when(brokerController.getShouldStartTime()).thenReturn(brokerStartTime);
        PopInflightMessageCounter counter = new PopInflightMessageCounter(brokerController);

        final String topic = "topic";
        final String group = "group";

        assertEquals(0, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.incrementInFlightMessageNum(topic, group, 0, 3);
        assertEquals(3, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.decrementInFlightMessageNum(topic, group, System.currentTimeMillis(), 0, 1);
        assertEquals(2, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.decrementInFlightMessageNum(topic, group, System.currentTimeMillis() - 1000, 0, 1);
        assertEquals(2, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        PopCheckPoint popCheckPoint = new PopCheckPoint();
        popCheckPoint.setTopic(topic);
        popCheckPoint.setCId(group);
        popCheckPoint.setQueueId(0);
        popCheckPoint.setPopTime(System.currentTimeMillis());

        counter.decrementInFlightMessageNum(popCheckPoint);
        assertEquals(1, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.decrementInFlightMessageNum(topic, group, System.currentTimeMillis(), 0 ,1);
        assertEquals(0, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.decrementInFlightMessageNum(topic, group, System.currentTimeMillis(), 0, 1);
        assertEquals(0, counter.getGroupPopInFlightMessageNum(topic, group, 0));
    }

    @Test
    public void testClearInFlightMessageNum() {
        BrokerController brokerController = mock(BrokerController.class);
        long brokerStartTime = System.currentTimeMillis();
        when(brokerController.getShouldStartTime()).thenReturn(brokerStartTime);
        PopInflightMessageCounter counter = new PopInflightMessageCounter(brokerController);

        final String topic = "topic";
        final String group = "group";

        assertEquals(0, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.incrementInFlightMessageNum(topic, group, 0, 3);
        assertEquals(3, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.clearInFlightMessageNumByTopicName("errorTopic");
        assertEquals(3, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.clearInFlightMessageNumByTopicName(topic);
        assertEquals(0, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.incrementInFlightMessageNum(topic, group, 0, 3);
        assertEquals(3, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.clearInFlightMessageNumByGroupName("errorGroup");
        assertEquals(3, counter.getGroupPopInFlightMessageNum(topic, group, 0));

        counter.clearInFlightMessageNumByGroupName(group);
        assertEquals(0, counter.getGroupPopInFlightMessageNum(topic, group, 0));
    }
}