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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;

public abstract class AbstractTransactionalMessageCheckListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private BrokerController brokerController;

    //queue nums of topic TRANS_CHECK_MAX_TIME_TOPIC
    protected final static int TCMT_QUEUE_NUMS = 1;

    private volatile ExecutorService executorService;

    public AbstractTransactionalMessageCheckListener() {
    }

    public AbstractTransactionalMessageCheckListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * Send a transaction status check request to the producer that originated
     * the half message.
     *
     * <p>Before sending, the message headers are restored to the real
     * business topic and queue so the producer can identify the original
     * message. The network request carries commitLogOffset, msgId,
     * transactionId, and queue offset for the producer to look up the local
     * transaction state. If the producer's channel is no longer connected,
     * the check is skipped with a warning.
     */
    public void sendCheckMessage(MessageExt msgExt) throws Exception {
        // format request header and message
        CheckTransactionStateRequestHeader checkTransactionStateRequestHeader = new CheckTransactionStateRequestHeader();
        checkTransactionStateRequestHeader.setTopic(msgExt.getTopic());
        checkTransactionStateRequestHeader.setCommitLogOffset(msgExt.getCommitLogOffset());
        checkTransactionStateRequestHeader.setOffsetMsgId(msgExt.getMsgId());
        checkTransactionStateRequestHeader.setMsgId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        checkTransactionStateRequestHeader.setTransactionId(checkTransactionStateRequestHeader.getMsgId());
        checkTransactionStateRequestHeader.setTranStateTableOffset(msgExt.getQueueOffset());
        checkTransactionStateRequestHeader.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
        msgExt.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgExt.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgExt.setStoreSize(0);

        // find channel, channel can send message to client
        String groupId = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        Channel channel = brokerController.getProducerManager().getAvailableChannel(groupId);

        if (channel != null) {
            // invoke channel.writeAndFlush() -> GrpcClientChannel.processCheckTransaction()
            brokerController.getBroker2Client().checkProducerTransactionState(groupId, channel, checkTransactionStateRequestHeader, msgExt);
        } else {
            LOGGER.warn("Check transaction failed, channel is null. groupId={}", groupId);
        }
    }

    /**
     * Asynchronously contact the producer to check the status of a half
     * message (commit, rollback, or unknown).
     *
     * <p>This is invoked by the transaction check loop when a half message
     * has aged past its immunity window without an OP record. The actual
     * network request is dispatched to the dedicated
     * {@code Transaction-msg-check-thread} pool so the caller is not blocked.
     * If the pool is full, the {@code CallerRunsPolicy} will execute the task
     * on the caller thread, applying back-pressure to the check loop.
     */
    public void resolveHalfMsg(final MessageExt msgExt) {
        if (executorService != null) {
            // executorService thread pool(2~5 threads)
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        sendCheckMessage(msgExt);
                    } catch (Exception e) {
                        LOGGER.error("Send check message error!", e);
                    }
                }
            });
        } else {
            LOGGER.error("TransactionalMessageCheckListener not init");
        }
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public void shutdown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    public synchronized void initExecutorService() {
        if (executorService == null) {
            executorService = ThreadUtils.newThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2000),
                new ThreadFactoryImpl("Transaction-msg-check-thread", brokerController.getBrokerIdentity()), new CallerRunsPolicy());
        }
    }

    /**
     * Inject brokerController for this listener
     *
     * @param brokerController
     */
    public void setBrokerController(BrokerController brokerController) {
        this.brokerController = brokerController;
        initExecutorService();
    }

    /**
     * In order to avoid check back unlimited, we will discard the message that have been checked more than a certain
     * number of times.
     *
     * @param msgExt Message to be discarded.
     */
    public abstract void resolveDiscardMsg(MessageExt msgExt);
}
