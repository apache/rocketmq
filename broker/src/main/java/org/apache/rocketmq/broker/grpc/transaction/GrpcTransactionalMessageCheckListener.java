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

package org.apache.rocketmq.broker.grpc.transaction;

import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.RecoverOrphanedTransactionRequest;
import java.util.List;
import java.util.Random;
import org.apache.rocketmq.broker.transaction.queue.DefaultTransactionalMessageCheckListener;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.grpc.channel.GrpcClientChannelManager;
import org.apache.rocketmq.grpc.channel.GrpcClientObserver;
import org.apache.rocketmq.grpc.common.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcTransactionalMessageCheckListener extends DefaultTransactionalMessageCheckListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    private final GrpcClientChannelManager manager;

    public GrpcTransactionalMessageCheckListener(GrpcClientChannelManager manager) {
        super();
        this.manager = manager;
    }

    public void sendCheckMessage(MessageExt msgExt) throws Exception {
        msgExt.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgExt.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgExt.setStoreSize(0);

        Message message = Converter.buildMessage(msgExt);
        String messageId = msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        long tranStateTableOffset = msgExt.getQueueOffset();
        long commitLogOffset = msgExt.getCommitLogOffset();
        TransactionHandle handle = new TransactionHandle(messageId, tranStateTableOffset, commitLogOffset);
        MultiplexingResponse response = MultiplexingResponse.newBuilder()
            .setRecoverOrphanedTransactionRequest(RecoverOrphanedTransactionRequest.newBuilder()
                .setOrphanedTransactionalMessage(message)
                .setTransactionId(handle.encode())
                .build())
            .build();

        String groupId = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        // select response client
        List<String> clientIds = this.manager.getClientIds(groupId);
        if (clientIds.isEmpty()) {
            LOGGER.warn("grpc check transaction failed, no observer, msg: {}", msgExt);
            return;
        }

        String clientId = clientIds.get(new Random().nextInt(clientIds.size()));
        GrpcClientObserver clientChannel = this.manager.get(groupId, clientId);
        if (clientChannel == null) {
            LOGGER.warn("grpc check transaction failed, client observer is not exists. clientId: {}, group: {}", clientId, groupId);
            return;
        }
        // call client
        clientChannel.call(response);
    }
}
