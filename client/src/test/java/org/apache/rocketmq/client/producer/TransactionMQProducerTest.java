package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionMQProducerTest {
    @Test
    public void testCreate() {
        RPCHook rpcHook = new RPCHook() {
            @Override
            public void doBeforeRequest(String remoteAddr, RemotingCommand request) {

            }

            @Override
            public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

            }
        };

        TransactionListener transactionListener = new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return null;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return null;
            }
        };

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        TransactionMQProducer producer = DefaultMQProducer.builder().producerGroup("GID_1").enableMessageTrace("trace_topic")
                .namespace("ns").nameserverAddress("NS_ADDR:9876").rpcHook(rpcHook).sendMsgTimeout(10000)
                        .createTransactionProducer(transactionListener, executorService);

        assertThat(producer.getProducerGroup()).isEqualTo("GID_1");
        assertThat(producer.getTraceDispatcher()).isNotNull();
        assertThat(producer.getNamespace()).isEqualTo("ns");
        assertThat(producer.getNamesrvAddr()).isEqualTo("NS_ADDR:9876");
        assertThat(producer.getSendMsgTimeout()).isEqualTo(10000);
        assertThat(producer.getTransactionListener()).isEqualTo(transactionListener);
        assertThat(producer.getExecutorService()).isEqualTo(executorService);
    }

}
