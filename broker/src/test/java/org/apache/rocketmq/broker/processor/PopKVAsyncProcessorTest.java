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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

public class PopKVAsyncProcessorTest {
    private BrokerController broker;
    private ChannelHandlerContext context;
    private final AtomicReference<RemotingCommand> response = new AtomicReference<>();
    private final AtomicInteger responses = new AtomicInteger();

    @Before
    public void setup() throws Exception {
        broker = Mockito.mock(BrokerController.class, Mockito.RETURNS_DEEP_STUBS);
        BrokerConfig config = new BrokerConfig();
        config.setPopConsumerKVServiceEnable(true);
        config.setAppendAckAsync(false);
        config.setAppendCkAsync(false);
        Mockito.when(broker.getBrokerConfig()).thenReturn(config);
        Mockito.when(broker.getTopicConfigManager().selectTopicConfig("topic")).thenReturn(new TopicConfig("topic"));
        Mockito.when(broker.getMessageStore().getMinOffsetInQueue(anyString(), anyInt())).thenReturn(0L);
        Mockito.when(broker.getMessageStore().getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(100L);
        context = Mockito.mock(ChannelHandlerContext.class, Mockito.RETURNS_DEEP_STUBS);
    }

    private AckMessageProcessor ackProcessor() {
        return new AckMessageProcessor(broker) {
            @Override
            protected void doResponse(Channel channel, RemotingCommand request, RemotingCommand result) {
                response.set(result);
                responses.incrementAndGet();
            }
        };
    }

    private RemotingCommand ackRequest() {
        AckMessageRequestHeader header = new AckMessageRequestHeader();
        header.setTopic("topic");
        header.setConsumerGroup("group");
        header.setQueueId(0);
        header.setOffset(1L);
        header.setExtraInfo(ExtraInfoUtil.buildExtraInfo(0, 10, 30000, 0, "topic", "broker", 0) + " 1");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, header);
        request.makeCustomHeaderToNet();
        return request;
    }

    @Test(timeout = 10000)
    public void ackWaitsForPersistence() throws Exception {
        CompletableFuture<Boolean> persisted = new CompletableFuture<>();
        Mockito.when(broker.getPopConsumerService().ackAsync(anyLong(), anyLong(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(persisted);
        RemotingCommand request = ackRequest();
        Assert.assertNull(ackProcessor().processRequest(context, request));
        Assert.assertNull(response.get());
        persisted.complete(true);
        Assert.assertEquals(ResponseCode.SUCCESS, response.get().getCode());
        Assert.assertEquals(request.getOpaque(), response.get().getOpaque());
        Assert.assertEquals(1, responses.get());
    }

    @Test(timeout = 10000)
    public void ackReportsPersistenceFailure() throws Exception {
        CompletableFuture<Boolean> persisted = new CompletableFuture<>();
        Mockito.when(broker.getPopConsumerService().ackAsync(anyLong(), anyLong(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(persisted);
        Assert.assertNull(ackProcessor().processRequest(context, ackRequest()));
        persisted.completeExceptionally(new IllegalStateException("disk failed"));
        Assert.assertEquals(ResponseCode.SYSTEM_ERROR, response.get().getCode());
        Assert.assertEquals(1, responses.get());
    }

    @Test(timeout = 10000)
    public void batchAckWaitsForEveryEntryAndReportsFailure() throws Exception {
        CompletableFuture<Boolean> first = new CompletableFuture<>();
        CompletableFuture<Boolean> second = new CompletableFuture<>();
        Mockito.when(broker.getPopConsumerService().ackAsync(anyLong(), anyLong(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(first, second);
        BatchAck ack = new BatchAck();
        ack.setConsumerGroup("group");
        ack.setTopic("topic");
        ack.setRetry("0");
        ack.setQueueId(0);
        ack.setReviveQueueId(0);
        ack.setStartOffset(1);
        ack.setPopTime(10);
        ack.setInvisibleTime(30000);
        BitSet bits = new BitSet();
        bits.set(0, 2);
        ack.setBitSet(bits);
        BatchAckMessageRequestBody body = new BatchAckMessageRequestBody();
        body.setBrokerName("broker");
        body.setAcks(Arrays.asList(ack));
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.BATCH_ACK_MESSAGE, null);
        request.setBody(body.encode());
        Assert.assertNull(ackProcessor().processRequest(context, request));
        first.complete(true);
        Assert.assertNull(response.get());
        second.completeExceptionally(new IllegalStateException("disk failed"));
        Assert.assertEquals(ResponseCode.SYSTEM_ERROR, response.get().getCode());
        Assert.assertEquals(1, responses.get());
    }

    private void change(boolean fail) throws Exception {
        CompletableFuture<Void> persisted = new CompletableFuture<>();
        Mockito.when(broker.getPopConsumerService().changeInvisibilityDuration(anyLong(), anyLong(), anyLong(),
            anyLong(), anyString(), anyString(), anyInt(), anyLong(), anyBoolean())).thenReturn(persisted);
        ChangeInvisibleTimeProcessor processor = new ChangeInvisibleTimeProcessor(broker) {
            @Override
            protected void doResponse(Channel channel, RemotingCommand request, RemotingCommand result) {
                response.set(result);
                responses.incrementAndGet();
            }
        };
        ChangeInvisibleTimeRequestHeader header = new ChangeInvisibleTimeRequestHeader();
        header.setTopic("topic");
        header.setConsumerGroup("group");
        header.setQueueId(0);
        header.setOffset(1L);
        header.setInvisibleTime(60000L);
        header.setExtraInfo(ExtraInfoUtil.buildExtraInfo(0, 10, 30000, 0, "topic", "broker", 0) + " 1");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, header);
        request.makeCustomHeaderToNet();
        Assert.assertNull(processor.processRequest(context, request));
        Assert.assertNull(response.get());
        if (fail) {
            persisted.completeExceptionally(new IllegalStateException("disk failed"));
        } else {
            persisted.complete(null);
        }
        Assert.assertEquals(fail ? ResponseCode.SYSTEM_ERROR : ResponseCode.SUCCESS, response.get().getCode());
        Assert.assertEquals(request.getOpaque(), response.get().getOpaque());
        Assert.assertEquals(1, responses.get());
    }

    @Test(timeout = 10000)
    public void changeIsAsyncEvenWithLegacyAsyncFlagsDisabled() throws Exception {
        change(false);
    }

    @Test(timeout = 10000)
    public void changeReportsPersistenceFailure() throws Exception {
        change(true);
    }
}
