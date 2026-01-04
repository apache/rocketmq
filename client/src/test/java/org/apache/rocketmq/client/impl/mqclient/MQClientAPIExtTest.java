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

package org.apache.rocketmq.client.impl.mqclient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.utils.FutureUtils;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class MQClientAPIExtTest {
    MQClientAPIExt mqClientAPIExt;
    @Mock
    NettyRemotingClient remotingClientMock;

    @Before
    public void before() {
        mqClientAPIExt = Mockito.spy(new MQClientAPIExt(new ClientConfig(), new NettyClientConfig(), null, null));
        Mockito.when(mqClientAPIExt.getRemotingClient()).thenReturn(remotingClientMock);
        Mockito.when(remotingClientMock.invoke(anyString(), any(), anyLong())).thenReturn(FutureUtils.completeExceptionally(new RemotingTimeoutException("addr")));
    }

    @Test
    public void sendMessageAsync() {
        String topic = "test";
        Message msg = new Message(topic, "test".getBytes());
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setProducerGroup("test");
        requestHeader.setDefaultTopic("test");
        requestHeader.setDefaultTopicQueueNums(1);
        requestHeader.setQueueId(0);
        requestHeader.setSysFlag(0);
        requestHeader.setBornTimestamp(0L);
        requestHeader.setFlag(0);
        requestHeader.setProperties("test");
        requestHeader.setReconsumeTimes(0);
        requestHeader.setUnitMode(false);
        requestHeader.setBatch(false);
        CompletableFuture<SendResult> future = mqClientAPIExt.sendMessageAsync("127.0.0.1:10911", "test", msg, requestHeader, 10);
        assertThatThrownBy(future::get).getCause().isInstanceOf(RemotingTimeoutException.class);
    }

    @Test
    public void testUpdateConsumerOffsetAsync_Success() throws ExecutionException, InterruptedException {
        CompletableFuture<RemotingCommand> remotingFuture = new CompletableFuture<>();
        remotingFuture.complete(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, ""));
        doReturn(remotingFuture).when(remotingClientMock).invoke(anyString(), any(RemotingCommand.class), anyLong());

        CompletableFuture<Void> future = mqClientAPIExt.updateConsumerOffsetAsync("brokerAddr", new UpdateConsumerOffsetRequestHeader(), 3000L);

        assertNull("Future should be completed without exception", future.get());
    }

    @Test
    public void testUpdateConsumerOffsetAsync_Fail() throws InterruptedException {

        CompletableFuture<RemotingCommand> remotingFuture = new CompletableFuture<>();
        remotingFuture.complete(RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "QueueId is null, topic is testTopic"));
        doReturn(remotingFuture).when(remotingClientMock).invoke(anyString(), any(RemotingCommand.class), anyLong());

        CompletableFuture<Void> future = mqClientAPIExt.updateConsumerOffsetAsync("brokerAddr", new UpdateConsumerOffsetRequestHeader(), 3000L);

        try {
            future.get();
        } catch (ExecutionException e) {
            MQBrokerException customEx = (MQBrokerException) e.getCause();
            assertEquals(customEx.getResponseCode(), ResponseCode.SYSTEM_ERROR);
            assertEquals(customEx.getErrorMessage(), "QueueId is null, topic is testTopic");
        }
    }

    @Test
    public void testRecallMessageAsync_success() {
        String msgId = "msgId";
        RecallMessageRequestHeader requestHeader = new RecallMessageRequestHeader();
        requestHeader.setProducerGroup("group");
        requestHeader.setTopic("topic");
        requestHeader.setRecallHandle("handle");
        requestHeader.setBrokerName("brokerName");

        RemotingCommand response = RemotingCommand.createResponseCommand(RecallMessageResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        RecallMessageResponseHeader responseHeader = (RecallMessageResponseHeader) response.readCustomHeader();
        responseHeader.setMsgId(msgId);
        response.makeCustomHeaderToNet();
        CompletableFuture<RemotingCommand> remotingFuture = new CompletableFuture<>();
        remotingFuture.complete(response);
        doReturn(remotingFuture).when(remotingClientMock).invoke(anyString(), any(RemotingCommand.class), anyLong());

        String resultId =
            mqClientAPIExt.recallMessageAsync("brokerAddr", requestHeader, 3000L).join();
        Assert.assertEquals(msgId, resultId);
    }

    @Test
    public void testRecallMessageAsync_fail() {
        RecallMessageRequestHeader requestHeader = new RecallMessageRequestHeader();
        requestHeader.setProducerGroup("group");
        requestHeader.setTopic("topic");
        requestHeader.setRecallHandle("handle");
        requestHeader.setBrokerName("brokerName");

        CompletableFuture<RemotingCommand> remotingFuture = new CompletableFuture<>();
        remotingFuture.complete(RemotingCommand.createResponseCommand(ResponseCode.SERVICE_NOT_AVAILABLE, ""));
        doReturn(remotingFuture).when(remotingClientMock).invoke(anyString(), any(RemotingCommand.class), anyLong());

        CompletionException exception = Assert.assertThrows(CompletionException.class, () -> {
            mqClientAPIExt.recallMessageAsync("brokerAddr", requestHeader, 3000L).join();
        });
        Assert.assertTrue(exception.getCause() instanceof MQBrokerException);
        MQBrokerException cause = (MQBrokerException) exception.getCause();
        Assert.assertEquals(ResponseCode.SERVICE_NOT_AVAILABLE, cause.getResponseCode());
    }
}