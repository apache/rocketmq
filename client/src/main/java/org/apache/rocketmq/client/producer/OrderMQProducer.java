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
package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByConsistentHash;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * A simple Order message Producer Facade to send order messages, which is a subclass of DefaultMQProducer <br>
 *
 * The messages with the same value returned from method {@code shardingKey} will try to be sent to the same message
 * queue, user must extend this class and implement the abstract method shardingKey.
 *
 * <p> Please notice that this can not be guaranteed when the numbers of messages are changed.<br> When the number of
 * message queues change before all the messages with the same sharding key sent successfully, the messages queue picked
 * to sent to may be changed.<br> However, only as few as messages will be influenced thanks to the default selector
 * backed by consistent hash algorithm. </p>
 */
public abstract class OrderMQProducer extends DefaultMQProducer {
    private MessageQueueSelector selector = new SelectMessageQueueByConsistentHash();

    public OrderMQProducer() {
    }

    /**
     * Constructor specifying producer group.
     *
     * @param producerGroup Producer group, see the name-sake field.
     */
    public OrderMQProducer(String producerGroup) {
        super(producerGroup);
    }

    /**
     * Constructor specifying both producer group and RPC hook.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public OrderMQProducer(final String producerGroup, RPCHook rpcHook) {
        super(producerGroup, rpcHook);
    }

    /**
     * Constructor specifying the RPC hook.
     *
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public OrderMQProducer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
    }

    public MessageQueueSelector getSelector() {
        return selector;
    }

    public void setSelector(MessageQueueSelector selector) {
        this.selector = selector;
    }

    /**
     * Callback method to return a unique sharding key for each messages. The sharding key will be used to pick a
     * message queue. The messages with the same message queue will be sent to the same message queues.
     *
     * @param msg the message which is ready to send
     * @return the sharding key which will be used to select message queue
     */
    abstract String shardingKey(Message msg);

    @Override
    public SendResult send(Message msg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(msg, selector, shardingKey(msg));
    }

    @Override
    public SendResult send(Message msg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(msg, selector, shardingKey(msg), timeout);
    }

    @Override
    public void send(Message msg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        super.send(msg, selector, shardingKey(msg), sendCallback);
    }

    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        super.send(msg, selector, shardingKey(msg), sendCallback, timeout);
    }

    @Override
    public void sendOneway(Message msg)
        throws MQClientException, RemotingException, InterruptedException {
        super.sendOneway(msg, selector, shardingKey(msg));
    }
}
