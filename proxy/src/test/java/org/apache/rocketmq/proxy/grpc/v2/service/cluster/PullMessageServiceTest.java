package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.FilterExpression;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryOffsetPolicy;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import apache.rocketmq.v1.Resource;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.assertj.core.util.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class PullMessageServiceTest extends BaseServiceTest {

    private PullMessageService pullMessageService;

    @Override
    public void beforeEach() throws Throwable {
        pullMessageService = new PullMessageService(this.connectorManager);
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");
    }

    @Test
    public void testQueryOffset() throws Exception {
        Context ctx = Context.current();

        when(defaultClient.getMaxOffset(anyString(), anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(100L));
        when(defaultClient.searchOffset(anyString(), anyString(), anyInt(), anyLong())).thenReturn(CompletableFuture.completedFuture(50L));

        QueryOffsetResponse response = pullMessageService.queryOffset(ctx, QueryOffsetRequest.newBuilder()
            .setPartition(Partition.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName("topic")
                    .build())
                .setBroker(Broker.newBuilder().setName("brokerName").build())
                .build())
            .setPolicy(QueryOffsetPolicy.BEGINNING)
            .build()
        ).get();
        assertEquals(Code.OK.getNumber(), response.getCommon().getStatus().getCode());
        assertEquals(0, response.getOffset());

        response = pullMessageService.queryOffset(ctx, QueryOffsetRequest.newBuilder()
            .setPartition(Partition.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName("topic")
                    .build())
                .setBroker(Broker.newBuilder().setName("brokerName").build())
                .build())
            .setPolicy(QueryOffsetPolicy.END)
            .build()
        ).get();
        assertEquals(Code.OK.getNumber(), response.getCommon().getStatus().getCode());
        assertEquals(100, response.getOffset());

        response = pullMessageService.queryOffset(ctx, QueryOffsetRequest.newBuilder()
            .setPartition(Partition.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName("topic")
                    .build())
                .setBroker(Broker.newBuilder().setName("brokerName").build())
                .build())
            .setTimePoint(Timestamps.fromMillis(System.currentTimeMillis()))
            .setPolicy(QueryOffsetPolicy.TIME_POINT)
            .build()
        ).get();
        assertEquals(Code.OK.getNumber(), response.getCommon().getStatus().getCode());
        assertEquals(50, response.getOffset());
    }

    @Test
    public void testPullMessage() throws Exception {
        AtomicReference<PullMessageRequestHeader> headerRef = new AtomicReference<>();
        PullResult pullResult = new PullResult(
            PullStatus.FOUND,
            3,
            0,
            10,
            Lists.newArrayList(
                createMessageExt("msg1", "msg1"),
                createMessageExt("msg2", "msg2")
            )
        );
        doAnswer(mock -> {
            headerRef.set(mock.getArgument(1));
            return CompletableFuture.completedFuture(pullResult);
        }).when(readConsumerClient).pullMessage(anyString(), any());

        Context ctx = Context.current().withDeadlineAfter(3, TimeUnit.SECONDS, Executors.newSingleThreadScheduledExecutor());
        PullMessageResponse response = pullMessageService.pullMessage(ctx, PullMessageRequest.newBuilder()
            .setPartition(Partition.newBuilder()
                .setBroker(Broker.newBuilder()
                    .setName("brokerName")
                    .build())
                .setTopic(Resource.newBuilder()
                    .setName("topic")
                    .build())
                .build())
            .setFilterExpression(FilterExpression.newBuilder()
                .setExpression("msg1")
                .setType(FilterType.TAG)
                .build())
            .build())
        .get();

        assertEquals(Code.OK.getNumber(), response.getCommon().getStatus().getCode());
        assertEquals(1, response.getMessagesCount());
        assertEquals("msg1", response.getMessages(0).getSystemAttribute().getMessageId());
    }
}