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
package org.apache.rocketmq.broker.transaction;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class AbstractTransactionCheckListener {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private BrokerController brokerController;

    private static ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("Transaction-msg-check-thread");
            return thread;
        }
    });

    public AbstractTransactionCheckListener() {}

    public AbstractTransactionCheckListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void sendCheckMessage(MessageExt msgExt) throws Exception {
        CheckTransactionStateRequestHeader checkTransactionStateRequestHeader = new CheckTransactionStateRequestHeader();
        checkTransactionStateRequestHeader.setCommitLogOffset(msgExt.getCommitLogOffset());
        checkTransactionStateRequestHeader.setOffsetMsgId(msgExt.getMsgId());
        checkTransactionStateRequestHeader.setMsgId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        checkTransactionStateRequestHeader.setTransactionId(checkTransactionStateRequestHeader.getMsgId());
        checkTransactionStateRequestHeader.setTranStateTableOffset(msgExt.getQueueOffset());
        msgExt.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgExt.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgExt.setStoreSize(0);
        String groupId = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        Channel channel = brokerController.getProducerManager().getAvaliableChannel(groupId);
        if (channel != null) {
            brokerController.getBroker2Client().checkProducerTransactionState(groupId, channel, checkTransactionStateRequestHeader, msgExt);
        } else {
            log.warn("Check transaction failed, channel is null. groupId={}", groupId);
        }
    }

    public void resolveHalfMsg(final MessageExt msgExt) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sendCheckMessage(msgExt);
                } catch (Exception e) {
                    log.error("Send check message error!", e);
                }
            }
        });
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public void shutDown() {
        executorService.shutdown();
    }

    /**
     * Inject brokerController for this listener
     *
     * @param brokerController
     */
    public void setBrokerController(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * In order to avoid check back unlimited, we will discard this message after checked too many times
     *
     * @param msgExt Message to be discarded.
     */
    public abstract void resolveDiscardMsg(MessageExt msgExt);
}
