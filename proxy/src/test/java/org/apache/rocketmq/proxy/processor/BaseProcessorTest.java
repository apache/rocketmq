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

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@Ignore
@RunWith(MockitoJUnitRunner.Silent.class)
public class BaseProcessorTest extends InitConfigAndLoggerTest {
    protected static final Random RANDOM = new Random();

    @Mock
    protected MessagingProcessor messagingProcessor;
    @Mock
    protected ServiceManager serviceManager;
    @Mock
    protected MessageService messageService;
    @Mock
    protected TopicRouteService topicRouteService;
    @Mock
    protected ProducerManager producerManager;
    @Mock
    protected ConsumerManager consumerManager;
    @Mock
    protected TransactionService transactionService;
    @Mock
    protected ProxyRelayService proxyRelayService;
    @Mock
    protected MetadataService metadataService;
    @Mock
    protected ProducerProcessor producerProcessor;
    @Mock
    protected ConsumerProcessor consumerProcessor;
    @Mock
    protected TransactionProcessor transactionProcessor;
    @Mock
    protected ClientProcessor clientProcessor;

    public void before() throws Throwable {
        super.before();
        when(serviceManager.getMessageService()).thenReturn(messageService);
        when(serviceManager.getTopicRouteService()).thenReturn(topicRouteService);
        when(serviceManager.getProducerManager()).thenReturn(producerManager);
        when(serviceManager.getConsumerManager()).thenReturn(consumerManager);
        when(serviceManager.getTransactionService()).thenReturn(transactionService);
        when(serviceManager.getProxyRelayService()).thenReturn(proxyRelayService);
        when(serviceManager.getMetadataService()).thenReturn(metadataService);
        when(messagingProcessor.getMetadataService()).thenReturn(metadataService);
    }

    protected static ProxyContext createContext() {
        return ProxyContext.create();
    }

    protected static MessageExt createMessageExt(String topic, String tags, int reconsumeTimes, long invisibleTime) {
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(topic);
        messageExt.setTags(tags);
        messageExt.setReconsumeTimes(reconsumeTimes);
        messageExt.setBody(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        messageExt.setMsgId(MessageClientIDSetter.createUniqID());
        messageExt.setCommitLogOffset(RANDOM.nextInt(Integer.MAX_VALUE));
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_POP_CK,
            ExtraInfoUtil.buildExtraInfo(RANDOM.nextInt(Integer.MAX_VALUE), System.currentTimeMillis(), invisibleTime,
                RANDOM.nextInt(Integer.MAX_VALUE), topic, "mockBroker", RANDOM.nextInt(Integer.MAX_VALUE), RANDOM.nextInt(Integer.MAX_VALUE)));
        return messageExt;
    }

    protected static ReceiptHandle create(MessageExt messageExt) {
        String ckInfo = messageExt.getProperty(MessageConst.PROPERTY_POP_CK);
        if (ckInfo == null) {
            return null;
        }
        return ReceiptHandle.decode(ckInfo + MessageConst.KEY_SEPARATOR + messageExt.getCommitLogOffset());
    }
}
