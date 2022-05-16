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
package org.apache.rocketmq.proxy.service.message;

import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.service.transaction.TransactionId;

public abstract class AbstractMessageService implements MessageService {

    protected CompletableFuture<SendResult> processSendMessageResponseFuture(
        String brokerName,
        SendMessageRequestHeader requestHeader,
        CompletableFuture<SendResult> future) {
        return future.thenApply(sendResult -> {
            int tranType = MessageSysFlag.getTransactionValue(requestHeader.getSysFlag());
            if (SendStatus.SEND_OK.equals(sendResult.getSendStatus()) &&
                tranType == MessageSysFlag.TRANSACTION_PREPARED_TYPE &&
                StringUtils.isNotBlank(sendResult.getTransactionId())) {
                TransactionId transactionId = TransactionId.genByBrokerTransactionId(brokerName, sendResult);
                sendResult.setTransactionId(transactionId.getProxyTransactionId());
            }
            return sendResult;
        });
    }
}
