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
package org.apache.rocketmq.proxy.service.message;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;

import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BatchChangeInvisibleTimeRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterMessageServiceTest {

    private TopicRouteService topicRouteService;
    private ClusterMessageService clusterMessageService;

    @Before
    public void before() {
        this.topicRouteService = mock(TopicRouteService.class);
        MQClientAPIFactory mqClientAPIFactory = mock(MQClientAPIFactory.class);
        this.clusterMessageService = new ClusterMessageService(this.topicRouteService, mqClientAPIFactory);
    }

    @Test
    public void testNativeBatchTransportUsesHeaderScopeAfterExplicitOptIn() throws Exception {
        MQClientAPIFactory factory = mock(MQClientAPIFactory.class);
        MQClientAPIExt client = mock(MQClientAPIExt.class);
        when(factory.getClient()).thenReturn(client);
        when(topicRouteService.getBrokerAddr(any(), anyString())).thenReturn("127.0.0.1:10911");
        ArgumentCaptor<BatchChangeInvisibleTimeRequestBody> body = ArgumentCaptor.forClass(BatchChangeInvisibleTimeRequestBody.class);
        when(client.batchChangeInvisibleTimeAsync(anyString(), anyString(), anyString(), body.capture(),
            org.mockito.ArgumentMatchers.anyLong())).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
        ReceiptHandle handle = ReceiptHandle.builder().startOffset(0).retrieveTime(System.currentTimeMillis())
            .invisibleTime(60000).reviveQueueId(0).topicType(ReceiptHandle.NORMAL_TOPIC).brokerName("broker")
            .queueId(0).offset(1).commitLogOffset(1).build();
        new ClusterMessageService(topicRouteService, factory, true).batchChangeInvisibleTime(ProxyContext.create(),
            Arrays.asList(new ReceiptHandleMessage(handle, "first"), new ReceiptHandleMessage(handle, "second")),
            "group", "topic", 30000, 3000, false).get();
        org.mockito.Mockito.verify(client).batchChangeInvisibleTimeAsync(org.mockito.ArgumentMatchers.eq("127.0.0.1:10911"),
            org.mockito.ArgumentMatchers.eq("topic"), org.mockito.ArgumentMatchers.eq("group"), any(),
            org.mockito.ArgumentMatchers.eq(3000L));
        assertEquals(2, body.getValue().getEntries().size());
        String encoded = new String(body.getValue().encode(), StandardCharsets.UTF_8);
        org.junit.Assert.assertFalse(encoded.contains("consumerGroup"));
        org.junit.Assert.assertFalse(encoded.contains("\"topic\""));
    }

    @Test
    public void testAckMessageByInvalidBrokerNameHandle() throws Exception {
        when(topicRouteService.getBrokerAddr(any(), anyString())).thenThrow(new MQClientException(ResponseCode.TOPIC_NOT_EXIST, ""));
        try {
            this.clusterMessageService.ackMessage(
                ProxyContext.create(),
                ReceiptHandle.builder()
                    .startOffset(0L)
                    .retrieveTime(System.currentTimeMillis())
                    .invisibleTime(3000)
                    .reviveQueueId(1)
                    .topicType(ReceiptHandle.NORMAL_TOPIC)
                    .brokerName("notExistBroker")
                    .queueId(0)
                    .offset(123)
                    .commitLogOffset(0L)
                    .build(),
                MessageClientIDSetter.createUniqID(),
                new AckMessageRequestHeader(),
                3000);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof ProxyException);
            ProxyException proxyException = (ProxyException) e;
            assertEquals(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, proxyException.getCode());
        }
    }
}
