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
package org.apache.rocketmq.proxy.service.transaction;

import java.util.List;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.proxy.common.ProxyContext;

public interface TransactionService {

    void addTransactionSubscription(String group, List<String> topicList);

    void addTransactionSubscription(String group, String topic);

    void replaceTransactionSubscription(String group, List<String> topicList);

    void unSubscribeAllTransactionTopic(String group);

    TransactionData addTransactionDataByBrokerAddr(String brokerAddr, String producerGroup, long tranStateTableOffset, long commitLogOffset, String transactionId,
        Message message);

    TransactionData addTransactionDataByBrokerName(String brokerName, String producerGroup, long tranStateTableOffset, long commitLogOffset, String transactionId,
        Message message);

    EndTransactionRequestData genEndTransactionRequestHeader(String producerGroup, Integer commitOrRollback,
        boolean fromTransactionCheck, String msgId, String transactionId);

    void onSendCheckTransactionStateFailed(ProxyContext context, String producerGroup, TransactionData transactionData);
}
