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
package org.apache.rocketmq.proxy.grpc.service.cluster;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.DefaultForwardClient;
import org.apache.rocketmq.proxy.connector.ForwardProducer;
import org.apache.rocketmq.proxy.connector.ForwardReadConsumer;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;
import org.apache.rocketmq.proxy.connector.ForwardWriteConsumer;
import org.apache.rocketmq.proxy.connector.transaction.TransactionHeartbeatRegisterService;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@Ignore
@RunWith(MockitoJUnitRunner.Silent.class)
public abstract class BaseServiceTest extends InitConfigAndLoggerTest {

    @Mock
    protected ConnectorManager connectorManager;
    @Mock
    protected DefaultForwardClient defaultClient;
    @Mock
    protected ForwardProducer producerClient;
    @Mock
    protected ForwardReadConsumer readConsumerClient;
    @Mock
    protected ForwardWriteConsumer writeConsumerClient;
    @Mock
    protected TopicRouteCache topicRouteCache;
    @Mock
    protected TransactionHeartbeatRegisterService transactionHeartbeatRegisterService;

    @Before
    public void before() throws Throwable {
        super.before();
        when(connectorManager.getDefaultForwardClient()).thenReturn(defaultClient);
        when(connectorManager.getForwardProducer()).thenReturn(producerClient);
        when(connectorManager.getForwardReadConsumer()).thenReturn(readConsumerClient);
        when(connectorManager.getForwardWriteConsumer()).thenReturn(writeConsumerClient);
        when(connectorManager.getTopicRouteCache()).thenReturn(topicRouteCache);
        when(connectorManager.getTransactionHeartbeatRegisterService()).thenReturn(transactionHeartbeatRegisterService);

        beforeEach();
    }

    public abstract void beforeEach() throws Throwable;

    protected static ReceiptHandle createReceiptHandle() {
        return ReceiptHandle.builder()
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName("brokerName")
            .retrieveTime(System.currentTimeMillis())
            .invisibleTime(TimeUnit.SECONDS.toMillis(3))
            .queueId(ThreadLocalRandom.current().nextInt(8))
            .offset(ThreadLocalRandom.current().nextInt(1000))
            .commitLogOffset(ThreadLocalRandom.current().nextInt(1000))
            .build();
    }

    protected static MessageExt createMessageExt(String msgId, String tag) {
        return createMessageExt(msgId, tag, createReceiptHandle().encode());
    }

    protected static MessageExt createMessageExt(String msgId, String tag, String handler) {
        SocketAddress addr = RemotingUtil.string2SocketAddress("127.0.0.1:8080");
        MessageExt msg = new MessageExt(0,
            System.currentTimeMillis(),
            addr,
            System.currentTimeMillis(),
            addr,
            msgId);
        msg.setTopic("topic");
        msg.setBody("hello".getBytes(StandardCharsets.UTF_8));
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TAGS, tag);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, msgId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_POP_CK, handler);
        return msg;
    }
}
