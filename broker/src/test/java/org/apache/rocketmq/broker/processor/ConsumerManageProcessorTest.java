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

package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Method;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumerManageProcessorTest {

    private BrokerController brokerController;
    private ConsumerOffsetManager consumerOffsetManager;
    private String normaCid = "normalCid";
    private String exceptionCid = "exceptionCid";

    @Test
    public void test_queryConsumerOffset_normal() throws Exception {

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.queryOffset(eq(normaCid), anyString(), anyInt())).thenReturn(10L);

        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(brokerController);

        RemotingCommand request = buildQueryCommond(normaCid);

        Method method = ConsumerManageProcessor.class.getDeclaredMethod("queryConsumerOffset", ChannelHandlerContext.class, RemotingCommand.class);
        method.setAccessible(true);

        final Object invoke = method.invoke(consumerManageProcessor, null, request);

        RemotingCommand response = (RemotingCommand) invoke;

        assertEquals(ResponseCode.SUCCESS, response.getCode());

    }

    @Test
    public void test_queryConsumerOffset_not_exists() throws Exception {

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.queryOffset(eq(exceptionCid), anyString(), anyInt())).thenReturn(-1L);

        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(brokerController);

        RemotingCommand request = buildQueryCommond(exceptionCid);

        Method method = ConsumerManageProcessor.class.getDeclaredMethod("queryConsumerOffset", ChannelHandlerContext.class, RemotingCommand.class);
        method.setAccessible(true);

        final Object invoke = method.invoke(consumerManageProcessor, null, request);

        RemotingCommand response = (RemotingCommand) invoke;

        assertEquals(ResponseCode.QUERY_NOT_FOUND, response.getCode());

    }

    @Before
    public void initMock() throws Exception {
        brokerController = mock(BrokerController.class);
        consumerOffsetManager = mock(ConsumerOffsetManager.class);
    }

    private RemotingCommand buildQueryCommond(String cid) {
        QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
        requestHeader.setConsumerGroup(cid);
        requestHeader.setTopic("topic");
        requestHeader.setQueueId(0);
        RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);
        requestCommand.makeCustomHeaderToNet();
        return requestCommand;
    }
}
