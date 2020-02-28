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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.client.rmq;

import org.apache.log4j.Logger;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.test.clientinterface.AbstractMQProducer;
import org.apache.rocketmq.test.sendresult.ResultWrapper;

public class RMQTransactionalProducer extends AbstractMQProducer {
    private static Logger logger  = Logger.getLogger(RMQTransactionalProducer.class);
    private TransactionMQProducer producer = null;
    private String nsAddr = null;

    public RMQTransactionalProducer(String nsAddr, String topic, TransactionListener transactionListener) {
        this(nsAddr, topic, false, transactionListener);
    }

    public RMQTransactionalProducer(String nsAddr, String topic, boolean useTLS, TransactionListener transactionListener) {
        super(topic);
        this.nsAddr = nsAddr;
        create(useTLS, transactionListener);
        start();
    }

    protected void create(boolean useTLS, TransactionListener transactionListener) {
        producer = new TransactionMQProducer();
        producer.setProducerGroup(getProducerGroupName());
        producer.setInstanceName(getProducerInstanceName());
        producer.setTransactionListener(transactionListener);
        producer.setUseTLS(useTLS);

        if (nsAddr != null) {
            producer.setNamesrvAddr(nsAddr);
        }
    }

    public void start() {
        try {
            producer.start();
            super.setStartSuccess(true);
        } catch (MQClientException e) {
            super.setStartSuccess(false);
            logger.error(e);
            e.printStackTrace();
        }
    }

    @Override
    public ResultWrapper send(Object msg, Object arg) {
        boolean commitMsg = ((Pair<Boolean, LocalTransactionState>) arg).getObject2() == LocalTransactionState.COMMIT_MESSAGE;
        org.apache.rocketmq.client.producer.SendResult metaqResult = null;
        Message message = (Message) msg;
        try {
            long start = System.currentTimeMillis();
            metaqResult = producer.sendMessageInTransaction(message, arg);
            this.msgRTs.addData(System.currentTimeMillis() - start);
            if (isDebug) {
                logger.info(metaqResult);
            }
            sendResult.setMsgId(metaqResult.getMsgId());
            sendResult.setSendResult(true);
            sendResult.setBrokerIp(metaqResult.getMessageQueue().getBrokerName());
            if (commitMsg) {
                msgBodys.addData(new String(message.getBody()));
            }
            originMsgs.addData(msg);
            originMsgIndex.put(new String(message.getBody()), metaqResult);
        } catch (MQClientException e) {
            if (isDebug) {
                e.printStackTrace();
            }

            sendResult.setSendResult(false);
            sendResult.setSendException(e);
            errorMsgs.addData(msg);
        }
        return sendResult;
    }

    @Override
    public void shutdown() {
        producer.shutdown();
    }

}
