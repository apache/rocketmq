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
package org.apache.rocketmq.tools.monitor;

import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.topic.OffsetMovedEvent;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class DefaultMonitorListenerTest {
    private DefaultMonitorListener defaultMonitorListener;

    @Before
    public void init() {
        defaultMonitorListener = mock(DefaultMonitorListener.class);
    }

    @Test
    public void testBeginRound() {
        defaultMonitorListener.beginRound();
    }

    @Test
    public void testReportUndoneMsgs() {
        UndoneMsgs undoneMsgs = new UndoneMsgs();
        undoneMsgs.setConsumerGroup("default-group");
        undoneMsgs.setTopic("unit-test");
        undoneMsgs.setUndoneMsgsDelayTimeMills(30000);
        undoneMsgs.setUndoneMsgsSingleMQ(1);
        undoneMsgs.setUndoneMsgsTotal(100);
        defaultMonitorListener.reportUndoneMsgs(undoneMsgs);
    }

    @Test
    public void testReportFailedMsgs() {
        FailedMsgs failedMsgs = new FailedMsgs();
        failedMsgs.setTopic("unit-test");
        failedMsgs.setConsumerGroup("default-consumer");
        failedMsgs.setFailedMsgsTotalRecently(2);
        defaultMonitorListener.reportFailedMsgs(failedMsgs);
    }

    @Test
    public void testReportDeleteMsgsEvent() {
        DeleteMsgsEvent deleteMsgsEvent = new DeleteMsgsEvent();
        deleteMsgsEvent.setEventTimestamp(System.currentTimeMillis());
        deleteMsgsEvent.setOffsetMovedEvent(new OffsetMovedEvent());
        defaultMonitorListener.reportDeleteMsgsEvent(deleteMsgsEvent);
    }

    @Test
    public void testReportConsumerRunningInfo() {
        TreeMap<String, ConsumerRunningInfo> criTable = new TreeMap<>();
        ConsumerRunningInfo consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.setSubscriptionSet(new TreeSet<SubscriptionData>());
        consumerRunningInfo.setStatusTable(new TreeMap<String, ConsumeStatus>());
        consumerRunningInfo.setSubscriptionSet(new TreeSet<SubscriptionData>());
        consumerRunningInfo.setMqTable(new TreeMap<MessageQueue, ProcessQueueInfo>());
        consumerRunningInfo.setProperties(new Properties());
        criTable.put("test", consumerRunningInfo);
        defaultMonitorListener.reportConsumerRunningInfo(criTable);
    }
}