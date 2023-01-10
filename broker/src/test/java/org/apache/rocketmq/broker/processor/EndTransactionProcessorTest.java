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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EndTransactionProcessorTest {

    private EndTransactionProcessor endTransactionProcessor;

    @Mock
    private ChannelHandlerContext handlerContext;

    @Spy
    private BrokerController
        brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(),
            new MessageStoreConfig());

    @Mock
    private MessageStore messageStore;

    @Mock
    private TransactionalMessageService transactionMsgService;

    @Before
    public void init() {
        brokerController.setMessageStore(messageStore);
        brokerController.setTransactionalMessageService(transactionMsgService);
        endTransactionProcessor = new EndTransactionProcessor(brokerController);
    }

    private OperationResult createResponse(int status) {
        OperationResult response = new OperationResult();
        response.setPrepareMessage(createDefaultMessageExt());
        response.setResponseCode(status);
        response.setResponseRemark(null);
        return response;
    }

    @Test
    public void testProcessRequest() throws RemotingCommandException {
        when(transactionMsgService.commitMessage(any(EndTransactionRequestHeader.class))).thenReturn(createResponse(ResponseCode.SUCCESS));
        when(messageStore.putMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        RemotingCommand request = createEndTransactionMsgCommand(MessageSysFlag.TRANSACTION_COMMIT_TYPE, false);
        RemotingCommand response = endTransactionProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_CheckMessage() throws RemotingCommandException {
        when(transactionMsgService.commitMessage(any(EndTransactionRequestHeader.class))).thenReturn(createResponse(ResponseCode.SUCCESS));
        when(messageStore.putMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        RemotingCommand request = createEndTransactionMsgCommand(MessageSysFlag.TRANSACTION_COMMIT_TYPE, true);
        RemotingCommand response = endTransactionProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_NotType() throws RemotingCommandException {
        RemotingCommand request = createEndTransactionMsgCommand(MessageSysFlag.TRANSACTION_NOT_TYPE, true);
        RemotingCommand response = endTransactionProcessor.processRequest(handlerContext, request);
        assertThat(response).isNull();
    }

    @Test
    public void testProcessRequest_RollBack() throws RemotingCommandException {
        when(transactionMsgService.rollbackMessage(any(EndTransactionRequestHeader.class))).thenReturn(createResponse(ResponseCode.SUCCESS));
        RemotingCommand request = createEndTransactionMsgCommand(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE, true);
        RemotingCommand response = endTransactionProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    private MessageExt createDefaultMessageExt() {
        MessageExt messageExt = new MessageExt();
        messageExt.setMsgId("12345678");
        messageExt.setQueueId(0);
        messageExt.setCommitLogOffset(123456789L);
        messageExt.setQueueOffset(1234);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_QUEUE_ID, "0");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_PRODUCER_GROUP, "testTransactionGroup");
        return messageExt;
    }

    private EndTransactionRequestHeader createEndTransactionRequestHeader(int status, boolean isCheckMsg) {
        EndTransactionRequestHeader header = new EndTransactionRequestHeader();
        header.setCommitLogOffset(123456789L);
        header.setFromTransactionCheck(isCheckMsg);
        header.setCommitOrRollback(status);
        header.setMsgId("12345678");
        header.setTransactionId("123");
        header.setProducerGroup("testTransactionGroup");
        header.setTranStateTableOffset(1234L);
        return header;
    }

    private RemotingCommand createEndTransactionMsgCommand(int status, boolean isCheckMsg) {
        EndTransactionRequestHeader header = createEndTransactionRequestHeader(status, isCheckMsg);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, header);
        request.makeCustomHeaderToNet();
        return request;
    }
}
