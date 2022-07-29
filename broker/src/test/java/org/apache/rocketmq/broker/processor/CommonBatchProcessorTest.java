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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CommonBatchProcessorTest extends BatchProtocol {
    private final int totalRequestNum = 20;
    private final Integer queue = 0;
    private final String producerGroup = "producer-group";
    private final String topicPrefix = "batch-protocol-";

    @Before
    public void init() throws Exception {
        this.brokerController = new BrokerController(
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig(),
                new MessageStoreConfig());
        assertThat(brokerController.initialize()).isTrue();
        brokerController.start();

        Channel mockChannel = mock(Channel.class);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress(1024));
        when(ctx.channel()).thenReturn(mockChannel);
    }

    @After
    public void after() {
        brokerController.getMessageStore().destroy();
        brokerController.shutdown();
    }

    @Test
    public void testWithCommonBatchHeader() throws Exception {
        Map<Integer, RemotingCommand> expectedRequests = new HashMap<>();
        for (int i = 0; i < totalRequestNum; i++) {
            String topic = topicPrefix + "-" + i;
            RemotingCommand childSendRequest = createSendRequest(producerGroup, topic, queue);
            expectedRequests.put(childSendRequest.getOpaque(), childSendRequest);
        }
        RemotingCommand batchRequest = RemotingCommand.mergeChildren(new ArrayList<>(expectedRequests.values()));
        makeBatchRequestHeader(batchRequest, RequestCode.SEND_MESSAGE);
        CompletableFuture<RemotingCommand> future = brokerController.getCommonBatchProcessor().asyncProcessRequest(ctx, batchRequest, callback);
        assertThat(future.isDone()).isTrue();
        RemotingCommand batchResponse = future.get();
        assertThat(batchResponse.getCode()).isEqualTo(SUCCESS);
    }

    @Test
    public void testWithoutCommonBatchHeader() throws Exception {
        Map<Integer, RemotingCommand> expectedRequests = new HashMap<>();
        for (int i = 0; i < totalRequestNum; i++) {
            String topic = topicPrefix + "-" + i;
            RemotingCommand childSendRequest = createSendRequest(producerGroup, topic, queue);
            expectedRequests.put(childSendRequest.getOpaque(), childSendRequest);
        }
        RemotingCommand batchRequest = RemotingCommand.mergeChildren(new ArrayList<>(expectedRequests.values()));
        CompletableFuture<RemotingCommand> future = brokerController.getCommonBatchProcessor().asyncProcessRequest(ctx, batchRequest, callback);
        assertThat(future.isDone()).isTrue();
        RemotingCommand batchResponse = future.get();
        assertThat(batchResponse.getCode()).isEqualTo(SYSTEM_ERROR);
    }
}
