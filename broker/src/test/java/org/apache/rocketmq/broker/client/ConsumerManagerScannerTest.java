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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManagerScannerTest {
    private ConsumerManager consumerManager;
    private String group = "FooBar";
    private String clientId = "clientId";
    private ClientChannelInfo clientInfo;
    private Map<ConsumerGroupEvent, List<ConsumerIdsChangeListenerData>> groupEventListMap = new HashMap<>();

    @Mock
    private Channel channel;

    @Before
    public void init() {
        clientInfo = new ClientChannelInfo(channel, clientId, LanguageCode.JAVA, 0);

        consumerManager = new ConsumerManager(new ConsumerIdsChangeListener() {
            @Override
            public void handle(ConsumerGroupEvent event, String group, Object... args) {
                groupEventListMap.compute(event, (eventKey, dataListVal) -> {
                    if (dataListVal == null) {
                        dataListVal = new ArrayList<>();
                    }
                    dataListVal.add(new ConsumerIdsChangeListenerData(event, group, args));
                    return dataListVal;
                });
            }

            @Override
            public void shutdown() {

            }
        }, 1000 * 120);
    }

    private static class ConsumerIdsChangeListenerData {
        private ConsumerGroupEvent event;
        private String group;
        private Object[] args;

        public ConsumerIdsChangeListenerData(ConsumerGroupEvent event, String group, Object[] args) {
            this.event = event;
            this.group = group;
            this.args = args;
        }
    }

    @Test
    public void testClientUnregisterEventInDoChannelCloseEvent() {
        assertThat(consumerManager.registerConsumer(
            group,
            clientInfo,
            ConsumeType.CONSUME_PASSIVELY,
            MessageModel.CLUSTERING,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            new HashSet<>(),
            false
        )).isTrue();

        consumerManager.doChannelCloseEvent("remoteAddr", channel);

        assertThat(groupEventListMap.get(ConsumerGroupEvent.CLIENT_UNREGISTER).size()).isEqualTo(1);
        assertThat(groupEventListMap.get(ConsumerGroupEvent.CLIENT_UNREGISTER).get(0).args[0]).isInstanceOf(ClientChannelInfo.class);
        ClientChannelInfo clientChannelInfo = (ClientChannelInfo) groupEventListMap.get(ConsumerGroupEvent.CLIENT_UNREGISTER).get(0).args[0];
        assertThat(clientChannelInfo).isSameAs(clientInfo);
    }

    @Test
    public void testClientUnregisterEventInUnregisterConsumer() {
        assertThat(consumerManager.registerConsumer(
            group,
            clientInfo,
            ConsumeType.CONSUME_PASSIVELY,
            MessageModel.CLUSTERING,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            new HashSet<>(),
            false
        )).isTrue();

        consumerManager.unregisterConsumer(group, clientInfo, false);

        assertThat(groupEventListMap.get(ConsumerGroupEvent.CLIENT_UNREGISTER).size()).isEqualTo(1);
        assertThat(groupEventListMap.get(ConsumerGroupEvent.CLIENT_UNREGISTER).get(0).args[0]).isInstanceOf(ClientChannelInfo.class);
        ClientChannelInfo clientChannelInfo = (ClientChannelInfo) groupEventListMap.get(ConsumerGroupEvent.CLIENT_UNREGISTER).get(0).args[0];
        assertThat(clientChannelInfo).isSameAs(clientInfo);
    }

    @Test
    public void testClientUnregisterEventInScanNotActiveChannel() {
        assertThat(consumerManager.registerConsumer(
            group,
            clientInfo,
            ConsumeType.CONSUME_PASSIVELY,
            MessageModel.CLUSTERING,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            new HashSet<>(),
            false
        )).isTrue();
        clientInfo.setLastUpdateTimestamp(0);
        when(channel.close()).thenReturn(mock(ChannelFuture.class));

        consumerManager.scanNotActiveChannel();
        assertThat(groupEventListMap.get(ConsumerGroupEvent.CLIENT_UNREGISTER).size()).isEqualTo(1);
        assertThat(groupEventListMap.get(ConsumerGroupEvent.CLIENT_UNREGISTER).get(0).args[0]).isInstanceOf(ClientChannelInfo.class);
        ClientChannelInfo clientChannelInfo = (ClientChannelInfo) groupEventListMap.get(ConsumerGroupEvent.CLIENT_UNREGISTER).get(0).args[0];
        assertThat(clientChannelInfo).isSameAs(clientInfo);
    }
}
