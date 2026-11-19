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
package org.apache.rocketmq.proxy.remoting.protocol.http2proxy;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class Http2ProxyBackendHandlerTest {

    @Test
    public void testExceptionClosesBothChannels() {
        EmbeddedChannel inboundChannel = new EmbeddedChannel();
        EmbeddedChannel backendChannel = new EmbeddedChannel(new Http2ProxyBackendHandler(inboundChannel));

        backendChannel.pipeline().fireExceptionCaught(new IllegalStateException("backend failure"));
        backendChannel.runPendingTasks();
        inboundChannel.runPendingTasks();

        assertFalse(backendChannel.isOpen());
        assertFalse(inboundChannel.isOpen());
        backendChannel.finishAndReleaseAll();
        inboundChannel.finishAndReleaseAll();
    }

    @Test
    public void testFailedForwardClosesBothChannels() {
        EmbeddedChannel inboundChannel = new EmbeddedChannel();
        inboundChannel.close();
        EmbeddedChannel backendChannel = new EmbeddedChannel(new Http2ProxyBackendHandler(inboundChannel));

        backendChannel.writeInbound(Unpooled.EMPTY_BUFFER);
        backendChannel.runPendingTasks();

        assertFalse(backendChannel.isOpen());
        assertFalse(inboundChannel.isOpen());
        backendChannel.finishAndReleaseAll();
        inboundChannel.finishAndReleaseAll();
    }
}
