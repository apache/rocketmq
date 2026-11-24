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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NettyRemotingClientCloseChannelTest {
    private static final String ADDR = "127.0.0.1:9876";
    private NettyRemotingClient client;

    @Before
    public void setUp() {
        client = new NettyRemotingClient(new NettyClientConfig());
    }

    @After
    public void tearDown() {
        client.shutdown();
    }

    @SuppressWarnings("unchecked")
    private Map<String, NettyRemotingClient.ChannelWrapper> channelTables() throws Exception {
        return (Map<String, NettyRemotingClient.ChannelWrapper>) FieldUtils.readField(client, "channelTables", true);
    }

    private ChannelFuture activeChannelFuture() {
        EmbeddedChannel channel = new EmbeddedChannel();
        DefaultChannelPromise promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        promise.setSuccess();
        return promise;
    }

    @Test
    public void testCloseChannelRemovesEntryWhenWrapperMatchesChannel() throws Exception {
        ChannelFuture future = activeChannelFuture();
        NettyRemotingClient.ChannelWrapper wrapper = client.new ChannelWrapper(ADDR, future);
        channelTables().put(ADDR, wrapper);

        client.closeChannel(ADDR, future.channel());

        assertThat(channelTables()).doesNotContainKey(ADDR);
    }

    @Test
    public void testCloseChannelKeepsEntryWhenWrapperRecreatedForAnotherChannel() throws Exception {
        // The table holds the wrapper for a freshly recreated channel. Closing an older,
        // unrelated channel for the same address must not evict the current entry.
        ChannelFuture current = activeChannelFuture();
        NettyRemotingClient.ChannelWrapper wrapper = client.new ChannelWrapper(ADDR, current);
        channelTables().put(ADDR, wrapper);

        ChannelFuture stale = activeChannelFuture();
        client.closeChannel(ADDR, stale.channel());

        assertThat(channelTables()).containsKey(ADDR);
        assertThat(channelTables().get(ADDR)).isSameAs(wrapper);
    }
}
