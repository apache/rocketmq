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
package org.apache.rocketmq.remoting.rpc;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RpcClientImplTest {

    @Mock
    private RemotingClient remotingClient;

    @Mock
    private ClientMetadata clientMetadata;

    private RpcClientImpl rpcClient;

    private MessageQueue mq;

    @Mock
    private RpcRequest request;

    private final long defaultTimeout = 3000L;

    @Before
    public void init() throws IllegalAccessException {
        rpcClient = new RpcClientImpl(clientMetadata, remotingClient);

        String defaultBroker = "brokerName";
        mq = new MessageQueue("defaultTopic", defaultBroker, 0);
        RpcRequestHeader header = mock(RpcRequestHeader.class);
        when(request.getHeader()).thenReturn(header);
        when(clientMetadata.getBrokerNameFromMessageQueue(mq)).thenReturn(defaultBroker);
        when(clientMetadata.findMasterBrokerAddr(any())).thenReturn("127.0.0.1:10911");
    }

    @Test
    public void testInvoke_PULL_MESSAGE() throws Exception {
        when(request.getCode()).thenReturn(RequestCode.PULL_MESSAGE);

        doAnswer(invocation -> {
            InvokeCallback callback = invocation.getArgument(3);
            RemotingCommand response = mock(RemotingCommand.class);
            when(response.getBody()).thenReturn("success".getBytes());
            PullMessageResponseHeader responseHeader = mock(PullMessageResponseHeader.class);
            when(response.decodeCommandCustomHeader(PullMessageResponseHeader.class)).thenReturn(responseHeader);
            callback.operationSucceed(response);
            return null;
        }).when(remotingClient).invokeAsync(
                any(),
                any(RemotingCommand.class),
                anyLong(),
                any(InvokeCallback.class));

        Future<RpcResponse> future = rpcClient.invoke(mq, request, defaultTimeout);
        RpcResponse actual = future.get();

        assertEquals(ResponseCode.SUCCESS, actual.getCode());
        assertEquals("success", new String((byte[]) actual.getBody()));
    }

    @Test
    public void testInvoke_GET_MIN_OFFSET() throws Exception {
        when(request.getCode()).thenReturn(RequestCode.GET_MIN_OFFSET);

        RemotingCommand responseCommand = mock(RemotingCommand.class);
        when(responseCommand.getBody()).thenReturn("1".getBytes());
        GetMinOffsetResponseHeader responseHeader = mock(GetMinOffsetResponseHeader.class);
        when(responseCommand.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class)).thenReturn(responseHeader);
        when(remotingClient.invokeSync(any(), any(RemotingCommand.class), anyLong())).thenReturn(responseCommand);

        Future<RpcResponse> future = rpcClient.invoke(mq, request, defaultTimeout);
        RpcResponse actual = future.get();

        assertEquals(ResponseCode.SUCCESS, actual.getCode());
        assertEquals("1", new String((byte[]) actual.getBody()));
    }

    @Test
    public void testInvoke_GET_MAX_OFFSET() throws Exception {
        when(request.getCode()).thenReturn(RequestCode.GET_MAX_OFFSET);

        RemotingCommand responseCommand = mock(RemotingCommand.class);
        when(responseCommand.getBody()).thenReturn("1000".getBytes());
        GetMaxOffsetResponseHeader responseHeader = mock(GetMaxOffsetResponseHeader.class);
        when(responseCommand.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class)).thenReturn(responseHeader);
        when(remotingClient.invokeSync(any(), any(RemotingCommand.class), anyLong())).thenReturn(responseCommand);

        Future<RpcResponse> future = rpcClient.invoke(mq, request, defaultTimeout);
        RpcResponse actual = future.get();

        assertEquals(ResponseCode.SUCCESS, actual.getCode());
        assertEquals("1000", new String((byte[]) actual.getBody()));
    }

    @Test
    public void testInvoke_SEARCH_OFFSET_BY_TIMESTAMP() throws Exception {
        when(request.getCode()).thenReturn(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP);

        RemotingCommand responseCommand = mock(RemotingCommand.class);
        when(responseCommand.getBody()).thenReturn("1000".getBytes());
        SearchOffsetResponseHeader responseHeader = mock(SearchOffsetResponseHeader.class);
        when(responseCommand.decodeCommandCustomHeader(SearchOffsetResponseHeader.class)).thenReturn(responseHeader);
        when(remotingClient.invokeSync(any(), any(RemotingCommand.class), anyLong())).thenReturn(responseCommand);

        Future<RpcResponse> future = rpcClient.invoke(mq, request, defaultTimeout);
        RpcResponse actual = future.get();

        assertEquals(ResponseCode.SUCCESS, actual.getCode());
        assertEquals("1000", new String((byte[]) actual.getBody()));
    }

    @Test
    public void testInvoke_GET_EARLIEST_MSG_STORETIME() throws Exception {
        when(request.getCode()).thenReturn(RequestCode.GET_EARLIEST_MSG_STORETIME);

        RemotingCommand responseCommand = mock(RemotingCommand.class);
        when(responseCommand.getBody()).thenReturn("10000".getBytes());
        GetEarliestMsgStoretimeResponseHeader responseHeader = mock(GetEarliestMsgStoretimeResponseHeader.class);
        when(responseCommand.decodeCommandCustomHeader(GetEarliestMsgStoretimeResponseHeader.class)).thenReturn(responseHeader);
        when(remotingClient.invokeSync(any(), any(RemotingCommand.class), anyLong())).thenReturn(responseCommand);

        Future<RpcResponse> future = rpcClient.invoke(mq, request, defaultTimeout);
        RpcResponse actual = future.get();

        assertEquals(ResponseCode.SUCCESS, actual.getCode());
        assertEquals("10000", new String((byte[]) actual.getBody()));
    }

    @Test
    public void testInvoke_QUERY_CONSUMER_OFFSET() throws Exception {
        when(request.getCode()).thenReturn(RequestCode.QUERY_CONSUMER_OFFSET);

        RemotingCommand responseCommand = mock(RemotingCommand.class);
        when(responseCommand.getBody()).thenReturn("1000".getBytes());
        QueryConsumerOffsetResponseHeader responseHeader = mock(QueryConsumerOffsetResponseHeader.class);
        when(responseCommand.decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class)).thenReturn(responseHeader);
        when(remotingClient.invokeSync(any(), any(RemotingCommand.class), anyLong())).thenReturn(responseCommand);

        Future<RpcResponse> future = rpcClient.invoke(mq, request, defaultTimeout);
        RpcResponse actual = future.get();

        assertEquals(ResponseCode.SUCCESS, actual.getCode());
        assertEquals("1000", new String((byte[]) actual.getBody()));
    }

    @Test
    public void testInvoke_UPDATE_CONSUMER_OFFSET() throws Exception {
        when(request.getCode()).thenReturn(RequestCode.UPDATE_CONSUMER_OFFSET);

        RemotingCommand responseCommand = mock(RemotingCommand.class);
        when(responseCommand.getBody()).thenReturn("success".getBytes());
        UpdateConsumerOffsetResponseHeader responseHeader = mock(UpdateConsumerOffsetResponseHeader.class);
        when(responseCommand.decodeCommandCustomHeader(UpdateConsumerOffsetResponseHeader.class)).thenReturn(responseHeader);
        when(remotingClient.invokeSync(any(), any(RemotingCommand.class), anyLong())).thenReturn(responseCommand);

        Future<RpcResponse> future = rpcClient.invoke(mq, request, defaultTimeout);
        RpcResponse actual = future.get();

        assertEquals(ResponseCode.SUCCESS, actual.getCode());
        assertEquals("success", new String((byte[]) actual.getBody()));
    }

    @Test
    public void testInvoke_GET_TOPIC_STATS_INFO() throws Exception {
        when(request.getCode()).thenReturn(RequestCode.GET_TOPIC_STATS_INFO);

        RemotingCommand responseCommand = mock(RemotingCommand.class);
        TopicStatsTable topicStatsTable = new TopicStatsTable();
        when(responseCommand.getBody()).thenReturn(topicStatsTable.encode());
        when(remotingClient.invokeSync(any(), any(RemotingCommand.class), anyLong())).thenReturn(responseCommand);

        Future<RpcResponse> future = rpcClient.invoke(mq, request, defaultTimeout);
        RpcResponse actual = future.get();

        assertEquals(ResponseCode.SUCCESS, actual.getCode());
        assertTrue(actual.getBody() instanceof TopicStatsTable);
    }

    @Test
    public void testInvoke_GET_TOPIC_CONFIG() throws Exception {
        when(request.getCode()).thenReturn(RequestCode.GET_TOPIC_CONFIG);

        RemotingCommand responseCommand = mock(RemotingCommand.class);
        TopicConfigAndQueueMapping topicConfigAndQueueMapping = new TopicConfigAndQueueMapping();
        when(responseCommand.getBody()).thenReturn(RemotingSerializable.encode(topicConfigAndQueueMapping));
        when(remotingClient.invokeSync(any(), any(RemotingCommand.class), anyLong())).thenReturn(responseCommand);

        Future<RpcResponse> future = rpcClient.invoke(mq, request, defaultTimeout);
        RpcResponse actual = future.get();

        assertEquals(ResponseCode.SUCCESS, actual.getCode());
        assertTrue(actual.getBody() instanceof TopicConfigAndQueueMapping);
    }
}
