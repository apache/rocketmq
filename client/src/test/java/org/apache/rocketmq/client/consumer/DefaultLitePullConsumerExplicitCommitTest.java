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

package org.apache.rocketmq.client.consumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class DefaultLitePullConsumerExplicitCommitTest {
    private enum CommitApi {
        MAP, SET, SYNC_MAP
    }

    @Parameterized.Parameters(name = "{0}, persist={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            {CommitApi.MAP, false},
            {CommitApi.MAP, true},
            {CommitApi.SET, false},
            {CommitApi.SET, true},
            {CommitApi.SYNC_MAP, false},
            {CommitApi.SYNC_MAP, true}
        });
    }

    private final CommitApi api;
    private final boolean persist;

    public DefaultLitePullConsumerExplicitCommitTest(CommitApi api, boolean persist) {
        this.api = api;
        this.persist = persist;
    }

    @Test
    public void testCommitReusedAssignedQueueWithNamespace() throws Exception {
        assertCommit("demo", "orders", "demo%orders", false);
    }

    @Test
    public void testCommitAlreadyNamespacedQueue() throws Exception {
        assertCommit("demo", "demo%orders", "demo%orders", false);
    }

    @Test
    public void testCommitQueueWithoutNamespace() throws Exception {
        assertCommit(null, "orders", "orders", false);
    }

    @Test
    public void testCommitBothFormsOfSameQueue() throws Exception {
        assertCommit("demo", "orders", "demo%orders", true);
    }

    @Test
    public void testCommitNullAndEmptyInputs() {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("demo", "LitePullCommitTest", null);
        commit(consumer, null, null);
        commit(consumer, Collections.emptyMap(), Collections.emptySet());
    }

    private void assertCommit(String namespace, String topic, String internalTopic, boolean bothForms) throws Exception {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(namespace, "LitePullCommitTest", null);
        MessageQueue queue = new MessageQueue(topic, "broker-a", 0);
        MessageQueue internalQueue = new MessageQueue(internalTopic, "broker-a", 0);
        Set<MessageQueue> queues = new HashSet<>(Collections.singleton(queue));
        consumer.assign(Collections.unmodifiableSet(queues));

        DefaultLitePullConsumerImpl impl = (DefaultLitePullConsumerImpl) FieldUtils.readDeclaredField(
            consumer, "defaultLitePullConsumerImpl", true);
        assertThat(impl.getAssignedMessageQueue().getAssignedMessageQueues()).containsExactly(internalQueue);
        impl.getAssignedMessageQueue().updateConsumeOffset(internalQueue, 1L);

        RemoteBrokerOffsetStore store = spy(new RemoteBrokerOffsetStore(mock(MQClientInstance.class), "LitePullCommitTest"));
        doNothing().when(store).updateConsumeOffsetToBroker(any(MessageQueue.class), anyLong(), anyBoolean());
        store.updateOffset(internalQueue, 0L, false);
        // Keep assignment and the offset cache real while avoiding background pull tasks.
        FieldUtils.writeDeclaredField(impl, "offsetStore", store, true);
        FieldUtils.writeDeclaredField(impl, "serviceState", ServiceState.RUNNING, true);

        Map<MessageQueue, Long> offsets = new LinkedHashMap<>();
        long callerOffset = bothForms ? 2L : 1L;
        if (bothForms) {
            queues.add(internalQueue);
            offsets.put(internalQueue, 1L);
        }
        offsets.put(queue, callerOffset);
        commit(consumer, Collections.unmodifiableMap(offsets), Collections.unmodifiableSet(queues));

        assertThat(store.readOffset(internalQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1L);
        if (persist) {
            verify(store).updateConsumeOffsetToBroker(internalQueue, 1L, true);
        } else {
            verify(store, never()).updateConsumeOffsetToBroker(any(MessageQueue.class), anyLong(), anyBoolean());
        }
        assertThat(queue.getTopic()).isEqualTo(topic);
        assertThat(queues).hasSize(bothForms ? 2 : 1);
        assertThat(queues.contains(queue)).isTrue();
        assertThat(offsets).hasSize(bothForms ? 2 : 1);
        assertThat(offsets.get(queue)).isEqualTo(callerOffset);
    }

    private void commit(DefaultLitePullConsumer consumer, Map<MessageQueue, Long> offsets, Set<MessageQueue> queues) {
        switch (api) {
            case MAP:
                consumer.commit(offsets, persist);
                break;
            case SET:
                consumer.commit(queues, persist);
                break;
            case SYNC_MAP:
                consumer.commitSync(offsets, persist);
                break;
            default:
                throw new AssertionError(api);
        }
    }
}
