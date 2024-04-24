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

package org.apache.rocketmq.proxy.processor;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.service.transaction.EndTransactionRequestData;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class TransactionProcessorTest extends BaseProcessorTest {

    private static final String PRODUCER_GROUP = "producerGroup";
    private TransactionProcessor transactionProcessor;

    @Before
    public void before() throws Throwable {
        super.before();
        this.transactionProcessor = new TransactionProcessor(this.messagingProcessor, this.serviceManager);
    }

    @Test
    public void testEndTransaction() throws Throwable {
        testEndTransaction(MessageSysFlag.TRANSACTION_COMMIT_TYPE, TransactionStatus.COMMIT);
        testEndTransaction(MessageSysFlag.TRANSACTION_NOT_TYPE, TransactionStatus.UNKNOWN);
        testEndTransaction(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE, TransactionStatus.ROLLBACK);
    }

    protected void testEndTransaction(int sysFlag, TransactionStatus transactionStatus) throws Throwable {
        when(this.messageService.endTransactionOneway(any(), any(), any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        ArgumentCaptor<Integer> commitOrRollbackCaptor = ArgumentCaptor.forClass(Integer.class);
        when(transactionService.genEndTransactionRequestHeader(any(), anyString(), anyString(), commitOrRollbackCaptor.capture(), anyBoolean(), anyString(), anyString()))
            .thenReturn(new EndTransactionRequestData("brokerName", new EndTransactionRequestHeader()));

        this.transactionProcessor.endTransaction(
            createContext(),
            "topic",
            "transactionId",
            "msgId",
            PRODUCER_GROUP,
            transactionStatus,
            true,
            3000
        );

        assertEquals(sysFlag, commitOrRollbackCaptor.getValue().intValue());

        reset(this.messageService);
    }
}
