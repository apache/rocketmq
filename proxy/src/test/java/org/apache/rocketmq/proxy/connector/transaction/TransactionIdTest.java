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