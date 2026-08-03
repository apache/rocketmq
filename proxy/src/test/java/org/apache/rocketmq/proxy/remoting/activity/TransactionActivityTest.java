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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.TransactionStatus;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionActivityTest extends InitConfigTest {

    private TransactionActivity transactionActivity;
    @Mock
    private MessagingProcessor messagingProcessor;
    @Spy
    private ChannelHandlerContext ctx = new SimpleChannelHandlerContext(new SimpleChannel(null, "0.0.0.0:0", "1.1.1.1:1")) {
        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return null;
        }
    };

    @Before
    public void setUp() {
        transactionActivity = new TransactionActivity(null, messagingProcessor);
        Channel channel = ctx.channel();
        RemotingHelper.setPropertyToAttr(channel, AttributeKeys.CLIENT_ID_KEY, "clientId");
        RemotingHelper.setPropertyToAttr(channel, AttributeKeys.LANGUAGE_CODE_KEY, LanguageCode.JAVA);
        RemotingHelper.setPropertyToAttr(channel, AttributeKeys.VERSION_KEY, MQVersion.CURRENT_VERSION);
    }

    @Test
    public void testEndTransactionWritesSuccessAfterFutureCompletes() throws Exception {
        when(messagingProcessor.endTransaction(any(), eq("topic"), eq("transactionId"), eq("msgId"),
            eq("producerGroup"), eq(TransactionStatus.COMMIT), eq(false)))
            .thenReturn(CompletableFuture.completedFuture(null));
        ArgumentCaptor<RemotingCommand> responseCaptor = ArgumentCaptor.forClass(RemotingCommand.class);

        RemotingCommand response = transactionActivity.processRequest0(ctx,
            createRequest(MessageSysFlag.TRANSACTION_COMMIT_TYPE), ProxyContext.create());

        assertThat(response).isNull();
        verify(ctx, times(1)).writeAndFlush(responseCaptor.capture());
        assertThat(responseCaptor.getValue().getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testEndTransactionWritesFailureWhenFutureFails() throws Exception {
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new MQBrokerException(ResponseCode.FLUSH_DISK_TIMEOUT, "flush timeout"));
        when(messagingProcessor.endTransaction(any(), anyString(), anyString(), anyString(), anyString(),
            any(TransactionStatus.class), anyBoolean()))
            .thenReturn(failedFuture);
        ArgumentCaptor<RemotingCommand> responseCaptor = ArgumentCaptor.forClass(RemotingCommand.class);

        RemotingCommand response = transactionActivity.processRequest0(ctx,
            createRequest(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE), ProxyContext.create());

        assertThat(response).isNull();
        verify(ctx, times(1)).writeAndFlush(responseCaptor.capture());
        assertThat(responseCaptor.getValue().getCode()).isEqualTo(ResponseCode.FLUSH_DISK_TIMEOUT);
        assertThat(responseCaptor.getValue().getRemark()).contains("flush timeout");
    }

    private RemotingCommand createRequest(int commitOrRollback) {
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        requestHeader.setTopic("topic");
        requestHeader.setTransactionId("transactionId");
        requestHeader.setMsgId("msgId");
        requestHeader.setProducerGroup("producerGroup");
        requestHeader.setTranStateTableOffset(1L);
        requestHeader.setCommitLogOffset(2L);
        requestHeader.setCommitOrRollback(commitOrRollback);
        requestHeader.setFromTransactionCheck(false);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }
}
