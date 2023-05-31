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
package org.apache.rocketmq.client.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.utils.MessageUtil;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerReplyMessageHookImpl implements ConsumeMessageHook {
    private final static Logger log = LoggerFactory.getLogger(ConsumerReplyMessageHookImpl.class);

    private final String consumerGroup;
    private final MQClientInstance mqClientInstance;

    private final ThreadPoolExecutor sendReplyExecutor;


    public ConsumerReplyMessageHookImpl(String consumerGroup, MQClientInstance mqClientInstance, int sendReplyMessageThreadNums) {
        this.consumerGroup = consumerGroup;
        this.mqClientInstance = mqClientInstance;
        this.sendReplyExecutor = new ThreadPoolExecutor(
                sendReplyMessageThreadNums,
                sendReplyMessageThreadNums,
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryImpl("SendReplyMessageThread_"),
                new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    @Override
    public String hookName() {
        return "ConsumerReplyMessageHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {

    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        List<MessageExt> msgList = context.getMsgList();
        for (MessageExt message : msgList) {
            if (MessageUtil.isReplyMsg(message)) {
                this.sendReplyExecutor.submit(() -> {
                    try {
                        replyMessageConsumerResultToBroker(context.getStatus(), message);
                    } catch (Exception e) {
                        log.warn("send reply message to broker exception, {}", e);
                    }
                });
            }
        }
    }

    private void replyMessageConsumerResultToBroker(String consumerResult, MessageExt message) throws RemotingTooMuchRequestException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        String ttl = message.getProperty(MessageConst.PROPERTY_MESSAGE_REPLY_TTL);
        String sendTime = message.getProperty(MessageConst.PROPERTY_MESSAGE_REPLY_SEND_TIME);
        if (StringUtils.isAnyBlank(ttl, sendTime)) {
            log.warn("message property 'REPLY_TTL' or 'REPLY_SEND_TIME' is be null, can not reply message");
            return;
        }

        long replyTimeout = Long.parseLong(sendTime) + Long.parseLong(ttl);
        if (System.currentTimeMillis() > replyTimeout) {
            log.warn("reply message timeout: " + replyTimeout + ", can not reply message");
            return;
        }

        ReplyMessageRequestHeader requestHeader = new ReplyMessageRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setConsumerResult(consumerResult);
        requestHeader.setConsumerTimeStamp(System.currentTimeMillis());
        requestHeader.setTopic(message.getTopic());
        requestHeader.setFlag(message.getFlag());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));

        String brokerAddress = this.mqClientInstance.findBrokerAddressInPublish(message.getBrokerName());
        if (StringUtils.isNoneBlank(brokerAddress)) {
            this.mqClientInstance.getMQClientAPIImpl().replyMessageConsumerResultToBroker(brokerAddress, requestHeader, message.getBody());
        }
    }
}
