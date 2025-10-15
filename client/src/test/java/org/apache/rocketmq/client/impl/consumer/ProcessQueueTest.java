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
package org.apache.rocketmq.client.impl.consumer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ProcessQueueTest {

    @Test
    public void testCachedMessageCount() {
        ProcessQueue pq = new ProcessQueue();

        pq.putMessage(createMessageList());

        assertThat(pq.getMsgCount().get()).isEqualTo(100);

        pq.takeMessages(10);
        pq.commit();

        assertThat(pq.getMsgCount().get()).isEqualTo(90);

        pq.removeMessage(Collections.singletonList(pq.getMsgTreeMap().lastEntry().getValue()));
        assertThat(pq.getMsgCount().get()).isEqualTo(89);
    }

    @Test
    public void testCachedMessageSize() {
        ProcessQueue pq = new ProcessQueue();

        pq.putMessage(createMessageList());

        assertThat(pq.getMsgSize().get()).isEqualTo(100 * 123);

        pq.takeMessages(10);
        pq.commit();

        assertThat(pq.getMsgSize().get()).isEqualTo(90 * 123);

        pq.removeMessage(Collections.singletonList(pq.getMsgTreeMap().lastEntry().getValue()));
        assertThat(pq.getMsgSize().get()).isEqualTo(89 * 123);
    }

    @Test
    public void testContainsMessage() {
        ProcessQueue pq = new ProcessQueue();
        final List<MessageExt> messageList = createMessageList(2);
        final MessageExt message0 = messageList.get(0);
        final MessageExt message1 = messageList.get(1);

        pq.putMessage(Lists.list(message0));
        assertThat(pq.containsMessage(message0)).isTrue();
        assertThat(pq.containsMessage(message1)).isFalse();
    }

    @Test
    public void testFillProcessQueueInfo() throws IllegalAccessException {
        ProcessQueue pq = new ProcessQueue();
        pq.putMessage(createMessageList(102400));

        ProcessQueueInfo processQueueInfo = new ProcessQueueInfo();
        pq.fillProcessQueueInfo(processQueueInfo);

        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(12);

        pq.takeMessages(10000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(10);

        pq.takeMessages(10000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(9);

        pq.takeMessages(80000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(0);

        TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<>();
        consumingMsgOrderlyTreeMap.put(0L, createMessageList(1).get(0));
        FieldUtils.writeDeclaredField(pq, "consumingMsgOrderlyTreeMap", consumingMsgOrderlyTreeMap, true);
        pq.fillProcessQueueInfo(processQueueInfo);
        assertEquals(0, processQueueInfo.getTransactionMsgMinOffset());
        assertEquals(0, processQueueInfo.getTransactionMsgMaxOffset());
        assertEquals(1, processQueueInfo.getTransactionMsgCount());
    }

    @Test
    public void testPopRequest() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        ProcessQueue processQueue = createProcessQueue();
        MessageExt messageExt = createMessageList(1).get(0);
        messageExt.getProperties().put(MessageConst.PROPERTY_CONSUME_START_TIMESTAMP, System.currentTimeMillis() - 20 * 60 * 1000L + "");
        processQueue.getMsgTreeMap().put(0L, messageExt);
        DefaultMQPushConsumer pushConsumer = mock(DefaultMQPushConsumer.class);
        processQueue.cleanExpiredMsg(pushConsumer);
        verify(pushConsumer).sendMessageBack(any(MessageExt.class), eq(3));
    }

    @Test
    public void testRollback() throws IllegalAccessException {
        ProcessQueue processQueue = createProcessQueue();
        processQueue.rollback();
        Field consumingMsgOrderlyTreeMapField = FieldUtils.getDeclaredField(processQueue.getClass(), "consumingMsgOrderlyTreeMap", true);
        TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = (TreeMap<Long, MessageExt>) consumingMsgOrderlyTreeMapField.get(processQueue);
        assertEquals(0, consumingMsgOrderlyTreeMap.size());
    }

    @Test
    public void testHasTempMessage() {
        ProcessQueue processQueue = createProcessQueue();
        assertFalse(processQueue.hasTempMessage());
    }

    @Test
    public void testProcessQueue() {
        ProcessQueue processQueue1 = createProcessQueue();
        ProcessQueue processQueue2 = createProcessQueue();
        assertEquals(processQueue1.getMsgAccCnt(), processQueue2.getMsgAccCnt());
        assertEquals(processQueue1.getTryUnlockTimes(), processQueue2.getTryUnlockTimes());
        assertEquals(processQueue1.getLastPullTimestamp(), processQueue2.getLastPullTimestamp());
    }

    private ProcessQueue createProcessQueue() {
        ProcessQueue result = new ProcessQueue();
        result.setMsgAccCnt(1);
        result.incTryUnlockTimes();
        return result;
    }

    private List<MessageExt> createMessageList() {
        return createMessageList(100);
    }

    private List<MessageExt> createMessageList(int count) {
        List<MessageExt> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MessageExt messageExt = new MessageExt();
            messageExt.setQueueOffset(i);
            messageExt.setBody(new byte[123]);
            messageExt.setKeys("keys" + i);
            messageExt.getProperties().put(MessageConst.PROPERTY_CONSUME_START_TIMESTAMP, System.currentTimeMillis() + "");
            result.add(messageExt);
        }
        return result;
    }
}
