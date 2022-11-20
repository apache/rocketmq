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
package org.apache.rocketmq.broker.util;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;

import java.util.concurrent.CompletableFuture;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return null;
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        return null;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt messageExt) {
        return false;
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return null;
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return null;
    }

    @Override
    public void check(long transactionTimeout, int transactionCheckMax, AbstractTransactionalMessageCheckListener listener) {
        log.warn("check check!");
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }
}
