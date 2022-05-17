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

import java.util.Random;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.service.transaction.TransactionId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.reset;

public class TransactionProcessorTest extends BaseProcessorTest {

    private Random random = new Random();
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
        ArgumentCaptor<EndTransactionRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(EndTransactionRequestHeader.class);
        doNothing().when(this.messageService).endTransactionOneway(any(), any(), requestHeaderArgumentCaptor.capture(), anyLong());

        TransactionId transactionId = TransactionId.genByBrokerTransactionId(
            "brokerName",
            "orgTxId",
            random.nextLong(),
            random.nextLong()
        );
        this.transactionProcessor.endTransaction(
            createContext(),
            transactionId,
            "msgId",
            PRODUCER_GROUP,
            transactionStatus,
            true,
            3000
        );

        EndTransactionRequestHeader requestHeader = requestHeaderArgumentCaptor.getValue();
        assertEquals(sysFlag, requestHeader.getCommitOrRollback().intValue());
        assertEquals(transactionId.getBrokerTransactionId(), requestHeader.getTransactionId());
        assertEquals(transactionId.getCommitLogOffset(), requestHeader.getCommitLogOffset().longValue());
        assertEquals(transactionId.getTranStateTableOffset(), requestHeader.getTranStateTableOffset().longValue());

        reset(this.messageService);
    }
}