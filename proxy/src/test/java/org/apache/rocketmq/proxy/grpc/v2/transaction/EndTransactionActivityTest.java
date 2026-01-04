/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.TransactionResolution;
import apache.rocketmq.v2.TransactionSource;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.processor.TransactionStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class EndTransactionActivityTest extends BaseActivityTest {

    private EndTransactionActivity endTransactionActivity;
    private TransactionResolution resolution;
    private TransactionSource source;
    private TransactionStatus transactionStatus;
    private Boolean fromTransactionCheck;

    public EndTransactionActivityTest(TransactionResolution resolution, TransactionSource source,
        TransactionStatus transactionStatus, Boolean fromTransactionCheck) {
        this.resolution = resolution;
        this.source = source;
        this.transactionStatus = transactionStatus;
        this.fromTransactionCheck = fromTransactionCheck;
    }

    @Before
    public void before() throws Throwable {
        super.before();
        this.endTransactionActivity = new EndTransactionActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void testEndTransaction() throws Throwable {
        ArgumentCaptor<TransactionStatus> transactionStatusCaptor = ArgumentCaptor.forClass(TransactionStatus.class);
        ArgumentCaptor<Boolean> fromTransactionCheckCaptor = ArgumentCaptor.forClass(Boolean.class);
        when(this.messagingProcessor.endTransaction(any(), any(), anyString(), anyString(), anyString(),
            transactionStatusCaptor.capture(),
            fromTransactionCheckCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        EndTransactionResponse response = this.endTransactionActivity.endTransaction(
            createContext(),
            EndTransactionRequest.newBuilder()
                .setResolution(resolution)
                .setTopic(Resource.newBuilder().setName("topic").build())
                .setMessageId(MessageClientIDSetter.createUniqID())
                .setTransactionId(MessageClientIDSetter.createUniqID())
                .setSource(source)
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(transactionStatus, transactionStatusCaptor.getValue());
        assertEquals(fromTransactionCheck, fromTransactionCheckCaptor.getValue());
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        Object[][] p = new Object[][] {
            {TransactionResolution.COMMIT, TransactionSource.SOURCE_CLIENT, TransactionStatus.COMMIT, false},
            {TransactionResolution.ROLLBACK, TransactionSource.SOURCE_SERVER_CHECK, TransactionStatus.ROLLBACK, true},
            {TransactionResolution.TRANSACTION_RESOLUTION_UNSPECIFIED, TransactionSource.SOURCE_SERVER_CHECK, TransactionStatus.UNKNOWN, true},
        };
        return Arrays.asList(p);
    }
}