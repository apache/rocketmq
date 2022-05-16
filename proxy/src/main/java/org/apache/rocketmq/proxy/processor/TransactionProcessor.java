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
package org.apache.rocketmq.proxy.processor;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.transaction.TransactionId;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class TransactionProcessor extends AbstractProcessor {

    public TransactionProcessor(MessagingProcessor messagingProcessor,
        ServiceManager serviceManager) {
        super(messagingProcessor, serviceManager);
    }

    void endTransaction(ProxyContext ctx, TransactionId transactionId, String messageId,String producerGroup,
        TransactionStatus transactionStatus, boolean fromTransactionCheck, long timeoutMillis) throws MQBrokerException, RemotingException, InterruptedException {

        EndTransactionRequestHeader requestHeader = buildEndTransactionRequestHeader(transactionId, messageId,
            producerGroup, transactionStatus, fromTransactionCheck);
        this.serviceManager.getMessageService().endTransactionOneway(
            ctx,
            transactionId,
            requestHeader,
            timeoutMillis
        );
    }

    protected EndTransactionRequestHeader buildEndTransactionRequestHeader(TransactionId transactionId, String messageId,String producerGroup,
        TransactionStatus transactionStatus, boolean fromTransactionCheck) {
        long transactionStateTableOffset = transactionId.getTranStateTableOffset();
        long commitLogOffset = transactionId.getCommitLogOffset();

        int commitOrRollback;
        switch (transactionStatus) {
            case COMMIT:
                commitOrRollback = MessageSysFlag.TRANSACTION_COMMIT_TYPE;
                break;
            case ROLLBACK:
                commitOrRollback = MessageSysFlag.TRANSACTION_ROLLBACK_TYPE;
                break;
            default:
                commitOrRollback = MessageSysFlag.TRANSACTION_NOT_TYPE;
                break;
        }

        EndTransactionRequestHeader endTransactionRequestHeader = new EndTransactionRequestHeader();
        endTransactionRequestHeader.setProducerGroup(producerGroup);
        endTransactionRequestHeader.setMsgId(messageId);
        endTransactionRequestHeader.setTransactionId(transactionId.getBrokerTransactionId());
        endTransactionRequestHeader.setTranStateTableOffset(transactionStateTableOffset);
        endTransactionRequestHeader.setCommitLogOffset(commitLogOffset);
        endTransactionRequestHeader.setCommitOrRollback(commitOrRollback);
        endTransactionRequestHeader.setFromTransactionCheck(fromTransactionCheck);

        return endTransactionRequestHeader;
    }

    public void addTransactionSubscription(ProxyContext ctx, String producerGroup, String topic) {
        this.serviceManager.getTransactionService().addTransactionSubscription(producerGroup, topic);
    }
}
