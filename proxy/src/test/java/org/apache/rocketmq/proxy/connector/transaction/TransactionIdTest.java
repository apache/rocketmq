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
package org.apache.rocketmq.proxy.connector.transaction;

import java.net.UnknownHostException;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransactionIdTest {

    @Test
    public void test() throws UnknownHostException {
        TransactionId transactionId = TransactionId.genByBrokerTransactionId(
            RemotingHelper.string2SocketAddress("127.0.0.1:8080"),
            "71F99B78B6E261357FA259CCA6456118", 1234, 5678);

        TransactionId decodeTransactionId = TransactionId.decode(transactionId.getProxyTransactionId());

        assertEquals(transactionId.getBrokerTransactionId(), decodeTransactionId.getBrokerTransactionId());
        assertEquals(transactionId.getBrokerAddr().toString(), decodeTransactionId.getBrokerAddr().toString());
        assertEquals(transactionId.getCommitLogOffset(), decodeTransactionId.getCommitLogOffset());
        assertEquals(transactionId.getTranStateTableOffset(), decodeTransactionId.getTranStateTableOffset());
    }

    @Test
    public void testEmptyTransactionId() throws UnknownHostException {
        TransactionId transactionId = TransactionId.genByBrokerTransactionId(
            RemotingHelper.string2SocketAddress("127.0.0.1:8080"),
            "", 1234, 5678);

        TransactionId decodeTransactionId = TransactionId.decode(transactionId.getProxyTransactionId());

        assertEquals(transactionId.getBrokerTransactionId(), decodeTransactionId.getBrokerTransactionId());
        assertEquals(transactionId.getBrokerAddr().toString(), decodeTransactionId.getBrokerAddr().toString());
        assertEquals(transactionId.getCommitLogOffset(), decodeTransactionId.getCommitLogOffset());
        assertEquals(transactionId.getTranStateTableOffset(), decodeTransactionId.getTranStateTableOffset());
    }

    @Test
    public void testNullTransactionId() throws UnknownHostException {
        TransactionId transactionId = TransactionId.genByBrokerTransactionId(
            RemotingHelper.string2SocketAddress("127.0.0.1:8080"),
            null, 1234, 5678);

        TransactionId decodeTransactionId = TransactionId.decode(transactionId.getProxyTransactionId());

        assertEquals("", decodeTransactionId.getBrokerTransactionId());
        assertEquals(transactionId.getBrokerAddr().toString(), decodeTransactionId.getBrokerAddr().toString());
        assertEquals(transactionId.getCommitLogOffset(), decodeTransactionId.getCommitLogOffset());
        assertEquals(transactionId.getTranStateTableOffset(), decodeTransactionId.getTranStateTableOffset());
    }
}