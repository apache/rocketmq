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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class OrderedConsumerOffsetTest {
    private final MessageQueue queue = new MessageQueue("OrderedTopic", "broker", 0);
    private final OffsetStore offsetStore = mock(OffsetStore.class);
    private final MQAdminImpl admin = mock(MQAdminImpl.class);
    private final PullAPIWrapper pullAPI = mock(PullAPIWrapper.class);
    private final PullMessageService pullService = mock(PullMessageService.class);
    private final PullRequest request = new PullRequest();
    private DefaultMQPushConsumerImpl consumer;

    @Before
    public void setUp() throws Exception {
        consumer = new DefaultMQPushConsumerImpl(new DefaultMQPushConsumer("ordered-group"), null);
        consumer.setServiceState(ServiceState.RUNNING);
        consumer.setConsumeOrderly(true);
        consumer.setOffsetStore(offsetStore);
        MQClientInstance client = mock(MQClientInstance.class);
        when(client.getMQAdminImpl()).thenReturn(admin);
        when(client.getPullMessageService()).thenReturn(pullService);
        FieldUtils.writeDeclaredField(consumer, "mQClientFactory", client, true);
        FieldUtils.writeDeclaredField(consumer, "pullAPIWrapper", pullAPI, true);
        RebalanceImpl rebalance = consumer.getRebalanceImpl();
        rebalance.setmQClientFactory(client);
        rebalance.getSubscriptionInner().put(queue.getTopic(), new SubscriptionData());
        when(offsetStore.readOffset(queue, ReadOffsetType.READ_FROM_STORE)).thenReturn(-1L);
        when(admin.maxOffset(queue)).thenReturn(9L);
        ProcessQueue processQueue = new ProcessQueue();
        processQueue.setLocked(true);
        request.setProcessQueue(processQueue);
        request.setMessageQueue(queue);
        request.setConsumerGroup("ordered-group");
        request.setNextOffset(rebalance.computePullFromWhereWithException(queue));
        assertThat(request.getNextOffset()).isEqualTo(9L);
        when(admin.maxOffset(queue)).thenReturn(10L);
    }

    @Test
    public void testMissingOffsetKeepsInitialBoundary() throws Exception {
        consumer.pullMessage(request);

        verifyPullOffset(9L);
        verify(admin).maxOffset(queue);
        assertThat(request.isPreviouslyLocked()).isTrue();
    }

    @Test
    public void testNewlyCommittedOffsetTakesPrecedence() throws Exception {
        when(offsetStore.readOffset(queue, ReadOffsetType.READ_FROM_STORE)).thenReturn(12L);

        consumer.pullMessage(request);

        verifyPullOffset(12L);
    }

    @Test
    public void testSmallerCommittedOffsetTakesPrecedence() throws Exception {
        when(offsetStore.readOffset(queue, ReadOffsetType.READ_FROM_STORE)).thenReturn(7L);

        consumer.pullMessage(request);

        verifyPullOffset(7L);
    }

    @Test
    public void testOffsetLookupFailureDelaysFirstPull() {
        when(offsetStore.readOffset(queue, ReadOffsetType.READ_FROM_STORE)).thenReturn(-2L);

        consumer.pullMessage(request);

        verifyNoInteractions(pullAPI);
        verify(pullService).executePullRequestLater(eq(request), anyLong());
        assertThat(request.isPreviouslyLocked()).isFalse();
        assertThat(request.getNextOffset()).isEqualTo(9L);
    }

    @Test
    public void testRetryAfterLookupFailureKeepsInitialBoundary() throws Exception {
        when(offsetStore.readOffset(queue, ReadOffsetType.READ_FROM_STORE)).thenReturn(-2L, -1L);

        consumer.pullMessage(request);
        consumer.pullMessage(request);

        verifyPullOffset(9L);
        verify(admin).maxOffset(queue);
        assertThat(request.isPreviouslyLocked()).isTrue();
    }

    private void verifyPullOffset(long offset) throws Exception {
        verify(pullAPI).pullKernelImpl(eq(queue), any(), any(), anyLong(), eq(offset),
            anyInt(), anyInt(), anyInt(), anyLong(), anyLong(), anyLong(),
            eq(CommunicationMode.ASYNC), any(PullCallback.class));
        assertThat(request.getNextOffset()).isEqualTo(offset);
    }
}
