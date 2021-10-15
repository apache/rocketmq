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

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;

import java.util.concurrent.ExecutorService;


public class MQProducerBuilder {
    /**
     * NameServer addresses to which this MQ Producer instance connects
     */
    private String nameserverAddress;

    /**
     * Namespace for this MQ Producer instance.
     */
    private String namespace;

    /**
     * Producer group conceptually aggregates all producer instances of exactly same role, which is particularly
     * important when transactional messages are involved. </p>
     * <p>
     * For non-transactional messages, it does not matter as long as it's unique per process. </p>
     * <p>
     * See {@linktourl http://rocketmq.apache.org/docs/core-concept/} for more discussion.
     */
    private String producerGroup = MixAll.DEFAULT_PRODUCER_GROUP;

    /**
     * RPC hook to execute per each remoting command execution
     */
    private RPCHook rpcHook;

    /**
     * Switch flag instance for message trace
     */
    private boolean enableMessageTrace = false;

    /**
     * The name value of message trace topic. If you don't config, you can use the default trace topic name.
     */
    private String customizedTraceTopic;

    /**
     * Timeout for sending messages.
     */
    private int sendMsgTimeout = 3000;

    public DefaultMQProducer createProducer() {
        DefaultMQProducer producer = new DefaultMQProducer(this.namespace, this.producerGroup, this.rpcHook,
                this.enableMessageTrace, this.customizedTraceTopic);

        if (null != nameserverAddress && !"".equals(nameserverAddress)) {
            producer.setNamesrvAddr(nameserverAddress);
        }
        producer.setSendMsgTimeout(sendMsgTimeout);
        return producer;
    }


    public TransactionMQProducer createTransactionProducer(TransactionListener transactionListener, ExecutorService executorService) {
        TransactionMQProducer producer = new TransactionMQProducer(this.namespace, this.producerGroup, this.rpcHook,
                this.enableMessageTrace, this.customizedTraceTopic);
        producer.setNamesrvAddr(this.nameserverAddress);
        producer.setSendMsgTimeout(this.sendMsgTimeout);
        producer.setTransactionListener(transactionListener);
        producer.setExecutorService(executorService);
        return producer;
    }

    public MQProducerBuilder nameserverAddress(final String nameserverAddr) {
        this.nameserverAddress = nameserverAddr;
        return this;
    }

    public MQProducerBuilder namespace(final String namespace) {
        this.namespace = namespace;
        return this;
    }

    public MQProducerBuilder producerGroup(final String producerGroup) {
        if (null != producerGroup && !"".equals(producerGroup)) {
            this.producerGroup = producerGroup;
        }
        return this;
    }

    public MQProducerBuilder rpcHook(final RPCHook rpcHook) {
        this.rpcHook = rpcHook;
        return this;
    }

    public MQProducerBuilder enableMessageTrace(final String customizedTraceTopic) {
        if (null != customizedTraceTopic && !"".equals(customizedTraceTopic)) {
            this.enableMessageTrace = true;
            this.customizedTraceTopic = customizedTraceTopic;
        }
        return this;

    }

    public MQProducerBuilder sendMsgTimeout(final int sendMsgTimeout) {
        if (sendMsgTimeout >= 0) {
            this.sendMsgTimeout = sendMsgTimeout;
        }
        return this;
    }


}
