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
package org.apache.rocketmq.proxy.grpc.v2.transaction;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.TransactionResolution;
import apache.rocketmq.v2.TransactionSource;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.TransactionStatus;

public class EndTransactionActivity extends AbstractMessingActivity {

    public EndTransactionActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    public CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx, EndTransactionRequest request) {
        CompletableFuture<EndTransactionResponse> future = new CompletableFuture<>();
        try {
            validateTopic(request.getTopic());
            if (StringUtils.isBlank(request.getTransactionId())) {
                throw new GrpcProxyException(Code.INVALID_TRANSACTION_ID, "transaction id cannot be empty");
            }

            TransactionStatus transactionStatus = TransactionStatus.UNKNOWN;
            TransactionResolution transactionResolution = request.getResolution();
            switch (transactionResolution) {
                case COMMIT:
                    transactionStatus = TransactionStatus.COMMIT;
                    break;
                case ROLLBACK:
                    transactionStatus = TransactionStatus.ROLLBACK;
                    break;
                default:
                    break;
            }
            future = this.messagingProcessor.endTransaction(
                ctx,
                request.getTopic().getName(),
                request.getTransactionId(),
                request.getMessageId(),
                request.getTopic().getName(),
                transactionStatus,
                request.getSource().equals(TransactionSource.SOURCE_SERVER_CHECK))
                .thenApply(r -> EndTransactionResponse.newBuilder()
                    .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                    .build());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }
}
