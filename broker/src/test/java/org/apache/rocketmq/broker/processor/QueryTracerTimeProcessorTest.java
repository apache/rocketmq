package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.ServerTracerTimeUtil;
import org.apache.rocketmq.common.TracerTime;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryTracerTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class QueryTracerTimeProcessorTest {
    @Mock
    private ChannelHandlerContext handlerContext;
    private QueryTracerTimeProcessor queryTracerTimeProcessor;

    @Before
    public void init() {
        queryTracerTimeProcessor = new QueryTracerTimeProcessor();
    }

    @Test
    public void test_no_exists() throws Exception {
        final RemotingCommand request = createQueryTrackerTimeCommand("");
        RemotingCommand responseToReturn = queryTracerTimeProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(responseToReturn.getBody()).isEqualTo(null);

    }

    @Test
    public void test_exists() throws Exception {
        long messageCreateTime = System.currentTimeMillis();
        TracerTime tracerTime = new TracerTime();
        tracerTime.setMessageCreateTime(messageCreateTime);

        String messageTracerTimeId = "messageTracerTimeId";

        final RemotingCommand request = createQueryTrackerTimeCommand(messageTracerTimeId);

        ServerTracerTimeUtil.tracerTimeCache.put(messageTracerTimeId, tracerTime);

        RemotingCommand responseToReturn = queryTracerTimeProcessor.processRequest(handlerContext, request);

        byte[] body = responseToReturn.getBody();
        TracerTime responseTracerTime = TracerTime.decode(body, TracerTime.class);

        assertThat(responseTracerTime).isEqualTo(tracerTime);
        assertThat(responseTracerTime.getMessageCreateTime()).isEqualTo(tracerTime.getMessageCreateTime());

    }

    private RemotingCommand createQueryTrackerTimeCommand(String messageTracerTimeId) {
        QueryTracerTimeRequestHeader requestHeader = new QueryTracerTimeRequestHeader();
        requestHeader.setMessageTracerTimeId(messageTracerTimeId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TRACER_TIME, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

}
