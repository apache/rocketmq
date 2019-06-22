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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultTransactionalMessageCheckListenerTest {

    private DefaultTransactionalMessageCheckListener listener;

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig());


    @Before
    public void init() {
        listener = new DefaultTransactionalMessageCheckListener();
        listener.setBrokerController(brokerController);
    }

    @Test
    public void testResolveHalfMsg() {
        listener.resolveHalfMsg(createMessageExt());
    }

    @Test
    public void testSendCheckMessage() throws Exception{
        MessageExt messageExt = createMessageExt();
        listener.sendCheckMessage(messageExt);
    }

    @Test
    public void sendCheckMessage(){
        listener.resolveDiscardMsg(createMessageExt());
    }

    private MessageExtBrokerInner createMessageExt() {
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        MessageAccessor.putProperty(inner,MessageConst.PROPERTY_REAL_QUEUE_ID,"1");
        MessageAccessor.putProperty(inner,MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,"1234255");
        MessageAccessor.putProperty(inner,MessageConst.PROPERTY_REAL_TOPIC,"realTopic");
        inner.setTransactionId(inner.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        inner.setBody("check".getBytes());
        inner.setMsgId("12344567890");
        inner.setQueueId(0);
        return inner;
    }

}
