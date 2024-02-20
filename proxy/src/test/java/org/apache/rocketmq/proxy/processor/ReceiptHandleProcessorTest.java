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
package org.apache.rocketmq.proxy.processor;

import io.netty.channel.local.LocalChannel;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ReceiptHandleProcessorTest extends BaseProcessorTest {
    private static final ProxyContext PROXY_CONTEXT = ProxyContext.create();
    private static final String CONSUMER_GROUP = "consumerGroup";
    private static final String TOPIC = "topic";
    private static final String BROKER_NAME = "broker";
    private static final int QUEUE_ID = 1;
    private static final String MESSAGE_ID = "messageId";
    private static final long OFFSET = 123L;
    private static final long INVISIBLE_TIME = 60000L;
    private static final int RECONSUME_TIMES = 1;
    private static final String MSG_ID = MessageClientIDSetter.createUniqID();
    private MessageReceiptHandle messageReceiptHandle;

    private ReceiptHandleProcessor receiptHandleProcessor;

    @Before
    public void before() throws Throwable {
        super.before();
        this.receiptHandleProcessor = new ReceiptHandleProcessor(this.messagingProcessor, this.serviceManager);
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        String receiptHandle = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(System.currentTimeMillis() - INVISIBLE_TIME + config.getRenewAheadTimeMillis() - 5)
            .invisibleTime(INVISIBLE_TIME)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(BROKER_NAME)
            .queueId(QUEUE_ID)
            .offset(OFFSET)
            .commitLogOffset(0L)
            .build().encode();
        PROXY_CONTEXT.withVal(ContextVariable.CLIENT_ID, "channel-id");
        PROXY_CONTEXT.withVal(ContextVariable.CHANNEL, new LocalChannel());
        Mockito.doNothing().when(consumerManager).appendConsumerIdsChangeListener(Mockito.any(ConsumerIdsChangeListener.class));
        messageReceiptHandle = new MessageReceiptHandle(CONSUMER_GROUP, TOPIC, QUEUE_ID, receiptHandle, MESSAGE_ID, OFFSET,
            RECONSUME_TIMES);
    }

    @Test
    public void testStartAutoRenewTask() throws Exception {
        receiptHandleProcessor.start();
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, PROXY_CONTEXT.getChannel(), CONSUMER_GROUP, MSG_ID, messageReceiptHandle);
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.any(), Mockito.eq(CONSUMER_GROUP))).thenReturn(new SubscriptionGroupConfig());
        Mockito.when(consumerManager.findChannel(Mockito.eq(CONSUMER_GROUP), Mockito.eq(PROXY_CONTEXT.getChannel()))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        Mockito.verify(messagingProcessor, Mockito.timeout(10000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(CONSUMER_GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getDefaultInvisibleTimeMills()));
    }

}
