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

import java.time.Duration;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

public abstract class AbstractTransactionService implements TransactionService, StartAndShutdown {

    protected TransactionDataManager transactionDataManager = new TransactionDataManager();

    @Override
    public TransactionData addTransactionDataByBrokerAddr(String brokerAddr, long tranStateTableOffset, long commitLogOffset, String transactionId,
        Message message) {
        return this.addTransactionDataByBrokerName(this.getBrokerNameByAddr(brokerAddr), tranStateTableOffset, commitLogOffset, transactionId, message);
    }

    @Override
    public TransactionData addTransactionDataByBrokerName(String brokerName, long tranStateTableOffset, long commitLogOffset, String transactionId,
        Message message) {
        if (StringUtils.isBlank(brokerName)) {
            return null;
        }
        long checkImmunityTime = parseCheckImmunityTime(message);
        TransactionData transactionData = new TransactionData(
            brokerName,
            tranStateTableOffset, commitLogOffset, transactionId,
            System.currentTimeMillis(), checkImmunityTime);

        this.transactionDataManager.addTransactionData(
            transactionId,
            transactionData
        );
        return transactionData;
    }

    @Override
    public EndTransactionRequestData genEndTransactionRequestHeader(String producerGroup, Integer commitOrRollback,
        boolean fromTransactionCheck, String msgId, String transactionId) {
        TransactionData transactionData = this.transactionDataManager.pollFirstNoExpireTransactionData(transactionId);
        if (transactionData == null) {
            return null;
        }
        EndTransactionRequestHeader header = new EndTransactionRequestHeader();
        header.setProducerGroup(producerGroup);
        header.setCommitOrRollback(commitOrRollback);
        header.setFromTransactionCheck(fromTransactionCheck);
        header.setMsgId(msgId);
        header.setTransactionId(transactionId);
        header.setTranStateTableOffset(transactionData.getTranStateTableOffset());
        header.setCommitLogOffset(transactionData.getCommitLogOffset());
        return new EndTransactionRequestData(transactionData.getBrokerName(), header);
    }

    @Override
    public void onSendCheckTransactionStateFailed(ProxyContext context, TransactionData transactionData) {
        this.transactionDataManager.removeTransactionData(transactionData.getTransactionId(), transactionData);
    }

    protected long parseCheckImmunityTime(Message message) {
        long checkImmunityTime = ConfigurationManager.getProxyConfig().getDefaultTransactionCheckImmunityTimeInMills();
        String checkImmunityTimeStr = message.getProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
        if (StringUtils.isNotBlank(checkImmunityTimeStr)) {
            try {
                checkImmunityTime = Duration.ofSeconds(Long.parseLong(checkImmunityTimeStr)).toMillis();
            } catch (Exception ignored) {
            }
        }
        return checkImmunityTime;
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
