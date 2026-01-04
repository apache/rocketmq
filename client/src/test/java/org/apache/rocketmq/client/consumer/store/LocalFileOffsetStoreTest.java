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
package org.apache.rocketmq.client.consumer.store;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LocalFileOffsetStoreTest {
    @Mock
    private MQClientInstance mQClientFactory;
    private String group = "FooBarGroup";
    private String topic = "FooBar";
    private String brokerName = "DefaultBrokerName";

    @Before
    public void init() {
        System.setProperty("rocketmq.client.localOffsetStoreDir", System.getProperty("java.io.tmpdir") + File.separator + ".rocketmq_offsets");
        String clientId = new ClientConfig().buildMQClientId() + "#TestNamespace" + System.currentTimeMillis();
        when(mQClientFactory.getClientId()).thenReturn(clientId);
    }

    @Test
    public void testUpdateOffset() throws Exception {
        OffsetStore offsetStore = new LocalFileOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 1);
        offsetStore.updateOffset(messageQueue, 1024, false);

        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1024);

        offsetStore.updateOffset(messageQueue, 1023, false);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1023);

        offsetStore.updateOffset(messageQueue, 1022, true);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1023);
    }

    @Test
    public void testReadOffset_FromStore() throws Exception {
        OffsetStore offsetStore = new LocalFileOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 2);

        offsetStore.updateOffset(messageQueue, 1024, false);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(-1);

        offsetStore.persistAll(new HashSet<>(Collections.singletonList(messageQueue)));
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1024);
    }

    @Test
    public void testCloneOffset() throws Exception {
        OffsetStore offsetStore = new LocalFileOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 3);
        offsetStore.updateOffset(messageQueue, 1024, false);
        Map<MessageQueue, Long> cloneOffsetTable = offsetStore.cloneOffsetTable(topic);

        assertThat(cloneOffsetTable.size()).isEqualTo(1);
        assertThat(cloneOffsetTable.get(messageQueue)).isEqualTo(1024);
    }

    @Test
    public void testPersist() throws Exception {
        OffsetStore offsetStore = new LocalFileOffsetStore(mQClientFactory, group);

        MessageQueue messageQueue0 = new MessageQueue(topic, brokerName, 0);
        offsetStore.updateOffset(messageQueue0, 1024, false);
        offsetStore.persist(messageQueue0);
        assertThat(offsetStore.readOffset(messageQueue0, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1024);

        MessageQueue messageQueue1 = new MessageQueue(topic, brokerName, 1);
        assertThat(offsetStore.readOffset(messageQueue1, ReadOffsetType.READ_FROM_STORE)).isEqualTo(-1);
    }

    @Test
    public void testPersistAll() throws Exception {
        OffsetStore offsetStore = new LocalFileOffsetStore(mQClientFactory, group);

        MessageQueue messageQueue0 = new MessageQueue(topic, brokerName, 0);
        offsetStore.updateOffset(messageQueue0, 1024, false);
        offsetStore.persistAll(new HashSet<MessageQueue>(Collections.singletonList(messageQueue0)));
        assertThat(offsetStore.readOffset(messageQueue0, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1024);

        MessageQueue messageQueue1 = new MessageQueue(topic, brokerName, 1);
        MessageQueue messageQueue2 = new MessageQueue(topic, brokerName, 2);
        offsetStore.updateOffset(messageQueue1, 1025, false);
        offsetStore.updateOffset(messageQueue2, 1026, false);
        offsetStore.persistAll(new HashSet<MessageQueue>(Arrays.asList(messageQueue1, messageQueue2)));

        assertThat(offsetStore.readOffset(messageQueue0, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1024);
        assertThat(offsetStore.readOffset(messageQueue1, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1025);
        assertThat(offsetStore.readOffset(messageQueue2, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1026);
    }

    @Test
    public void testRemoveOffset() throws Exception {
        OffsetStore offsetStore = new LocalFileOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 0);
        offsetStore.updateOffset(messageQueue, 1024, false);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1024);

        offsetStore.removeOffset(messageQueue);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(-1);
    }
}
