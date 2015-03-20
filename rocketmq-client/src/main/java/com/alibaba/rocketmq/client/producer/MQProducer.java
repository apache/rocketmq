/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.producer;

import com.alibaba.rocketmq.client.MQAdmin;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.util.List;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-25
 */
public interface MQProducer extends MQAdmin {
     void start() throws MQClientException;

     void shutdown();


     List<MessageQueue> fetchPublishMessageQueues(final String topic) throws MQClientException;


     SendResult send(final Message msg) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;


     SendResult send(final Message msg, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;


     void send(final Message msg, final SendCallback sendCallback) throws MQClientException,
            RemotingException, InterruptedException;


     void send(final Message msg, final SendCallback sendCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException;


     void sendOneway(final Message msg) throws MQClientException, RemotingException,
            InterruptedException;


     SendResult send(final Message msg, final MessageQueue mq) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;


     SendResult send(final Message msg, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;


     void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException;


     void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException;


     void sendOneway(final Message msg, final MessageQueue mq) throws MQClientException,
            RemotingException, InterruptedException;


     SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;


     SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg,
            final long timeout) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;


     void send(final Message msg, final MessageQueueSelector selector, final Object arg,
            final SendCallback sendCallback) throws MQClientException, RemotingException,
            InterruptedException;


     void send(final Message msg, final MessageQueueSelector selector, final Object arg,
            final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException,
            InterruptedException;


     void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, InterruptedException;


     TransactionSendResult sendMessageInTransaction(final Message msg,
            final LocalTransactionExecuter tranExecuter, final Object arg) throws MQClientException;
}
