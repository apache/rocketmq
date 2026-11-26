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

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.ChannelHandlerContext;
import java.time.Duration;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.TransactionStatus;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class TransactionActivity extends AbstractRemotingActivity {

    public TransactionActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        final EndTransactionRequestHeader requestHeader = (EndTransactionRequestHeader) request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
        // Remoting send requests are forwarded as is, so the proxy holds no transaction data for them.
        // Forward producer end transaction requests to the broker directly. Replies to transaction checks
        // still use the data recorded when the proxy relayed the check.
        if (!requestHeader.getFromTransactionCheck()) {
            return request(ctx, request, context, Duration.ofSeconds(3).toMillis());
        }

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        TransactionStatus transactionStatus = TransactionStatus.UNKNOWN;
        switch (requestHeader.getCommitOrRollback()) {
            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                transactionStatus = TransactionStatus.COMMIT;
                break;
            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                transactionStatus = TransactionStatus.ROLLBACK;
                break;
            default:
                break;
        }

        this.messagingProcessor.endTransaction(
            context,
            requestHeader.getTopic(),
            requestHeader.getTransactionId(),
            requestHeader.getMsgId(),
            requestHeader.getProducerGroup(),
            transactionStatus,
            requestHeader.getFromTransactionCheck()
        );
        return response;
    }
}
