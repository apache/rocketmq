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

import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.TransactionStatus;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransactionActivityTest extends InitConfigTest {
    private static final String TOPIC = "topic";
    private static final String BROKER_NAME = "broker-a";
    private final MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
    private final TransactionActivity activity = new TransactionActivity(null, messagingProcessor);
    private final ProxyContext context = ProxyContext.create();

    @Test
    public void testForwardProducerEndTransactionToBroker() throws Exception {
        for (int state : new int[] {
            MessageSysFlag.TRANSACTION_COMMIT_TYPE,
            MessageSysFlag.TRANSACTION_ROLLBACK_TYPE,
            MessageSysFlag.TRANSACTION_NOT_TYPE
        }) {
            RemotingCommand request = endTransactionRequest(state, BROKER_NAME, false);
            assertThat(activity.processRequest0(null, request, context)).isNull();
            verify(messagingProcessor).requestOneway(eq(context), eq(BROKER_NAME), same(request), anyLong());
        }
        verify(messagingProcessor, never()).endTransaction(any(), any(), any(), any(), any(), any(), anyBoolean());
    }

    @Test
    public void testProducerEndTransactionWithoutBrokerNameNotSupported() throws Exception {
        RemotingCommand request = endTransactionRequest(MessageSysFlag.TRANSACTION_COMMIT_TYPE, null, false);
        RemotingCommand response = activity.processRequest0(null, request, context);
        assertThat(response.getCode()).isEqualTo(ResponseCode.VERSION_NOT_SUPPORTED);
        verify(messagingProcessor, never()).requestOneway(any(), anyString(), any(), anyLong());
        verify(messagingProcessor, never()).endTransaction(any(), any(), any(), any(), any(), any(), anyBoolean());
    }

    @Test
    public void testTransactionCheckUsesStoredTransactionData() throws Exception {
        for (String brokerName : new String[] {null, BROKER_NAME}) {
            RemotingCommand request = endTransactionRequest(MessageSysFlag.TRANSACTION_COMMIT_TYPE, brokerName, true);
            activity.processRequest0(null, request, context);
        }
        verify(messagingProcessor, times(2)).endTransaction(context, TOPIC, "txId", "msgId", "group",
            TransactionStatus.COMMIT, true);
        verify(messagingProcessor, never()).requestOneway(any(), anyString(), any(), anyLong());
    }

    private RemotingCommand endTransactionRequest(int state, String brokerName, boolean fromTransactionCheck) {
        EndTransactionRequestHeader header = new EndTransactionRequestHeader();
        header.setBrokerName(brokerName);
        header.setTopic(TOPIC);
        header.setProducerGroup("group");
        header.setMsgId("msgId");
        header.setTransactionId("txId");
        header.setCommitLogOffset(123L);
        header.setTranStateTableOffset(45L);
        header.setCommitOrRollback(state);
        header.setFromTransactionCheck(fromTransactionCheck);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, header);
        request.markOnewayRPC();
        request.makeCustomHeaderToNet();
        return request;
    }
}
