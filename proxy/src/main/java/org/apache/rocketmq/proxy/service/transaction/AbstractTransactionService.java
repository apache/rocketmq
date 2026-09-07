/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;

public abstract class AbstractTransactionService implements TransactionService, StartAndShutdown {

    protected TransactionDataManager transactionDataManager = new TransactionDataManager();

    @Override
    public TransactionData addTransactionDataByBrokerAddr(ProxyContext ctx, String brokerAddr, String topic, String producerGroup, long tranStateTableOffset, long commitLogOffset, String transactionId,
        Message message) {
        return this.addTransactionDataByBrokerName(ctx, this.getBrokerNameByAddr(brokerAddr), topic, producerGroup, tranStateTableOffset, commitLogOffset, transactionId, message);
    }

    @Override
    public TransactionData addTransactionDataByBrokerName(ProxyContext ctx, String brokerName, String topic, String producerGroup, long tranStateTableOffset, long commitLogOffset, String transactionId,
        Message message) {
        if (StringUtils.isBlank(brokerName)) {
            return null;
        }
        TransactionData transactionData = new TransactionData(
            brokerName,
            topic,
            tranStateTableOffset, commitLogOffset, transactionId,
            System.currentTimeMillis(),
            ConfigurationManager.getProxyConfig().getTransactionDataExpireMillis());

        this.transactionDataManager.addTransactionData(
            producerGroup,
            transactionId,
            transactionData
        );
        return transactionData;
    }

    @Override
    public EndTransactionRequestData genEndTransactionRequestHeader(ProxyContext ctx, String topic, String producerGroup, Integer commitOrRollback, Long tranStateTableOffset, Long commitLogOffset,
        boolean fromTransactionCheck, String msgId, String transactionId) {
        EndTransactionRequestHeader header = new EndTransactionRequestHeader();
        header.setTopic(topic);
        header.setProducerGroup(producerGroup);
        header.setCommitOrRollback(commitOrRollback);
        header.setFromTransactionCheck(fromTransactionCheck);
        header.setMsgId(msgId);
        header.setTransactionId(transactionId);
        TransactionData transactionData = this.transactionDataManager.pollNoExpireTransactionData(producerGroup, transactionId);
        if (transactionData == null) {
            if (commitLogOffset != null && commitOrRollback != null) {
                header.setTranStateTableOffset(tranStateTableOffset);
                header.setCommitLogOffset(commitLogOffset);
                return new EndTransactionRequestData(transactionData.getBrokerName(), header);
            }
            return null;
        }

        header.setTranStateTableOffset(transactionData.getTranStateTableOffset());
        header.setCommitLogOffset(transactionData.getCommitLogOffset());
        return new EndTransactionRequestData(transactionData.getBrokerName(), header);
    }

    @Override
    public void onSendCheckTransactionStateFailed(ProxyContext context, String producerGroup, TransactionData transactionData) {
        this.transactionDataManager.removeTransactionData(producerGroup, transactionData.getTransactionId(), transactionData);
    }

    protected abstract String getBrokerNameByAddr(String brokerAddr);

    @Override
    public void shutdown() throws Exception {
        this.transactionDataManager.shutdown();
    }

    @Override
    public void start() throws Exception {
        this.transactionDataManager.start();
    }
}
