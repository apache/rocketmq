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

package org.apache.rocketmq.broker.offset;

import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;


@RunWith(MockitoJUnitRunner.class)
public class BrokerOffsetTest {


    @Mock
    private ClientChannelInfo clientChannelInfo;

    @Mock
    private BrokerController brokerController;

    @Mock
    private TopicConfigManager topicConfigManager;

    @Mock
    private ConsumerOffsetManager consumerOffsetManager;

    @Mock
    private MessageStore messageStore;

    @Mock
    private ConsumerManager consumerManager;

    @Mock
    private Channel channel;

    @Test
    public void testResetOffset() throws Exception {

        long offset  = 1006456;

        long maxOffset = 16565757;

        String topicName = "simple";

        String group = "myConsumeGroup";

        long time = System.currentTimeMillis();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);

        when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(topicConfigManager.selectTopicConfig(Mockito.anyString())).thenReturn(topicConfig);

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.queryOffset(Mockito.anyString(), Mockito.anyString(),Mockito.anyInt())).thenReturn(offset);

        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(messageStore.getOffsetInQueueByTime(Mockito.anyString(),Mockito.anyInt(),Mockito.anyLong())).thenReturn(0L);
        when(messageStore.getMaxOffsetInQueue(Mockito.anyString(),Mockito.anyInt())).thenReturn(maxOffset);

        when(clientChannelInfo.getChannel()).thenReturn(channel);
        when(clientChannelInfo.getVersion()).thenReturn(MQVersion.CURRENT_VERSION);

        ConsumerGroupInfo consumerGroupInfo = new ConsumerGroupInfo(null,null,null,null);

        consumerGroupInfo.updateChannel(clientChannelInfo, ConsumeType.CONSUME_PASSIVELY, MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        when(consumerManager.getConsumerGroupInfo(Mockito.anyString())).thenReturn(consumerGroupInfo);

        Broker2Client broker2Client = new Broker2Client(brokerController);

        RemotingCommand remotingCommand = broker2Client.resetOffset(topicName, group, time, true);
        ResetOffsetBody decode = ResetOffsetBody.decode(remotingCommand.getBody(), ResetOffsetBody.class);
        Map<MessageQueue, Long> offsetTable = decode.getOffsetTable();
        assertThat(offsetTable.entrySet().iterator().next().getValue()).isEqualTo(maxOffset);


        RemotingCommand remotingCommandNoForce = broker2Client.resetOffset(topicName, group, time, false);
        ResetOffsetBody decodeNoForce = ResetOffsetBody.decode(remotingCommandNoForce.getBody(), ResetOffsetBody.class);
        Map<MessageQueue, Long> offsetTableNoForce = decodeNoForce.getOffsetTable();
        assertThat(offsetTableNoForce.entrySet().iterator().next().getValue()).isEqualTo(offset);
    }


}
