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

import java.net.UnknownHostException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransactionIdTest {

    private static final String BROKER_NAME = "brokerName";

    @Test
    public void test() throws UnknownHostException {
        TransactionId transactionId = TransactionId.genByBrokerTransactionId(
            BROKER_NAME,
            "71F99B78B6E261357FA259CCA6456118", 1234, 5678);

        TransactionId decodeTransactionId = TransactionId.decode(transactionId.getProxyTransactionId());

        assertEquals(transactionId.getBrokerTransactionId(), decodeTransactionId.getBrokerTransactionId());
        assertEquals(transactionId.getBrokerName(), decodeTransactionId.getBrokerName());
        assertEquals(transactionId.getCommitLogOffset(), decodeTransactionId.getCommitLogOffset());
        assertEquals(transactionId.getTranStateTableOffset(), decodeTransactionId.getTranStateTableOffset());
    }

    @Test
    public void testEmptyTransactionId() throws UnknownHostException {
        TransactionId transactionId = TransactionId.genByBrokerTransactionId(
            BROKER_NAME,
            "", 1234, 5678);

        TransactionId decodeTransactionId = TransactionId.decode(transactionId.getProxyTransactionId());

        assertEquals(transactionId.getBrokerTransactionId(), decodeTransactionId.getBrokerTransactionId());
        assertEquals(transactionId.getBrokerName(), decodeTransactionId.getBrokerName());
        assertEquals(transactionId.getCommitLogOffset(), decodeTransactionId.getCommitLogOffset());
        assertEquals(transactionId.getTranStateTableOffset(), decodeTransactionId.getTranStateTableOffset());
    }

    @Test
    public void testNullTransactionId() throws UnknownHostException {
        TransactionId transactionId = TransactionId.genByBrokerTransactionId(
            BROKER_NAME,
            null, 1234, 5678);

        TransactionId decodeTransactionId = TransactionId.decode(transactionId.getProxyTransactionId());

        assertEquals("", decodeTransactionId.getBrokerTransactionId());
        assertEquals(transactionId.getBrokerName(), decodeTransactionId.getBrokerName());
        assertEquals(transactionId.getCommitLogOffset(), decodeTransactionId.getCommitLogOffset());
        assertEquals(transactionId.getTranStateTableOffset(), decodeTransactionId.getTranStateTableOffset());
    }
}