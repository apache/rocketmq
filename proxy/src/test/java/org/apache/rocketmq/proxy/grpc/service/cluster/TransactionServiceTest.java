package org.apache.rocketmq.proxy.grpc.service.cluster;

import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.PollCommandResponse;
import apache.rocketmq.v1.Resource;
import com.google.rpc.Code;
import io.grpc.Context;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.connector.transaction.TransactionId;
import org.apache.rocketmq.proxy.connector.transaction.TransactionStateCheckRequest;
import org.apache.rocketmq.proxy.grpc.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransactionServiceTest extends BaseServiceTest {

    private TransactionService transactionService;
    @Mock
    private ChannelManager channelManager;

    @Override
    public void beforeEach() throws Throwable {
        transactionService = new TransactionService(this.connectorManager, this.channelManager);
    }

    @Test
    public void testCheckTransactionState() {
        GrpcClientChannel channel = mock(GrpcClientChannel.class);
        AtomicReference<Object> writeDataRef = new AtomicReference<>();

        when(channelManager.getClientIdList(anyString())).thenReturn(Lists.newArrayList("clientId"));
        when(channelManager.getChannel(anyString(), any())).thenReturn(channel);
        doAnswer(mock -> {
            writeDataRef.set(mock.getArgument(0));
            return null;
        }).when(channel).writeAndFlush(any());

        TransactionId transactionId = TransactionId.genFromBrokerTransactionId(
            RemotingHelper.string2SocketAddress("127.0.0.1:8080"),
            "71F99B78B6E261357FA259CCA6456118", 1234, 5678);
        transactionService.checkTransactionState(new TransactionStateCheckRequest(
            "group",
            1L,
            2L,
            "msgId",
            transactionId,
            createMessageExt("msgId", "msgId")
        ));

        assertTrue(writeDataRef.get() instanceof PollCommandResponse);
        PollCommandResponse response = (PollCommandResponse) writeDataRef.get();
        assertEquals(transactionId.getProxyTransactionId(), response.getRecoverOrphanedTransactionCommand().getTransactionId());
    }

    @Test
    public void testEndTransaction() throws Exception {
        AtomicReference<EndTransactionRequestHeader> headerRef = new AtomicReference<>();
        AtomicReference<String> brokerAddrRef = new AtomicReference<>();
        TransactionId transactionId = TransactionId.genFromBrokerTransactionId(
            RemotingHelper.string2SocketAddress("127.0.0.1:8080"),
            "71F99B78B6E261357FA259CCA6456118", 1234, 5678);
        doAnswer(mock -> {
            brokerAddrRef.set(mock.getArgument(0));
            headerRef.set(mock.getArgument(1));
            return null;
        }).when(producerClient).endTransaction(anyString(), any(), anyLong());

        EndTransactionResponse response = transactionService.endTransaction(Context.current(), EndTransactionRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .setTransactionId(transactionId.getProxyTransactionId())
            .build()
        ).get();

        assertEquals(Code.OK.getNumber(), response.getCommon().getStatus().getCode());
        assertEquals(transactionId.getBrokerTransactionId(), headerRef.get().getTransactionId());
        assertEquals("127.0.0.1:8080", brokerAddrRef.get());
    }
}