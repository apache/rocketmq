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
package org.apache.rocketmq.proxy.remoting.pipeline;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContextInitPipelineTest {

    @Test
    public void testMalformedSimpleChannelAddressIsConsumedSafely() throws Exception {
        SimpleChannel channel = new SimpleChannel("127.0.0.1:65536", "127.0.0.1:-1");
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        ProxyContext context = ProxyContext.create();

        new ContextInitPipeline().execute(ctx, request, context);

        assertSame(channel, context.getChannel());
        assertEquals(ChannelProtocolType.REMOTING.getName(), context.getProtocolType());
        assertEquals("", context.getRemoteAddress());
        assertNull(context.getLocalAddress());
    }
}
