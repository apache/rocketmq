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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.CommonBatchRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.RemotingResponseCallback;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageStore;
import org.mockito.Mock;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public class BatchProtocol {
    protected BrokerController brokerController;
    protected RemotingResponseCallback callback = CompletableFuture::completedFuture;
    @Mock
    protected ChannelHandlerContext ctx;

    protected RemotingCommand createPullRequest(String consumerGroup, String topic, Integer queue, Long offset) {
        Integer maxMsgNums = 1000;

        PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queue);
        requestHeader.setQueueOffset(offset);
        requestHeader.setMaxMsgNums(maxMsgNums);
        requestHeader.setSysFlag(PullSysFlag.buildSysFlag(true, true, false, false));
        requestHeader.setCommitOffset(0L);
        requestHeader.setSuspendTimeoutMillis(10000L);
        requestHeader.setSubscription(null);
        requestHeader.setSubVersion(0L);
        requestHeader.setExpressionType(null);

        return RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
    }

    protected RemotingCommand createQueryOffsetRequest(String consumerGroup, String topic, Integer queue) {
        QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setQueueId(queue);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    protected RemotingCommand createUpdateConsumerOffsetRequest(String consumeGroup, String topic, Integer queue, Long offset) {
        UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
        requestHeader.setConsumerGroup(consumeGroup);
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queue);
        requestHeader.setCommitOffset(offset);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    protected RemotingCommand createSendRequest(String producerGroup, String topic, Integer queue) throws UnsupportedEncodingException {
        Message msg = new Message(topic,
                "TagA",
                "OrderID188",
                "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setTopic(topic);
        requestHeader.setDefaultTopic("");
        requestHeader.setDefaultTopicQueueNums(12);
        requestHeader.setQueueId(queue);
        requestHeader.setSysFlag(0);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(0);
        requestHeader.setReconsumeTimes(0);
        requestHeader.setUnitMode(false);
        requestHeader.setBatch(false);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        request.makeCustomHeaderToNet();
        request.setBody(msg.getBody());
        return request;
    }

    protected void makeBatchRequestHeader(RemotingCommand batchRequest, int dispatchCode) {
        CommonBatchRequestHeader requestHeader = new CommonBatchRequestHeader();
        requestHeader.setDispatchCode(dispatchCode);
        batchRequest.setHeader(requestHeader);
        batchRequest.setCode(RequestCode.COMMON_BATCH_REQUEST);
        batchRequest.makeCustomHeaderToNet();
    }

    protected Callable<Boolean> fullyDispatched(MessageStore messageStore) {
        return () -> messageStore.dispatchBehindBytes() == 0;
    }
}
