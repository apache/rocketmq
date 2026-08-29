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
package org.apache.rocketmq.proxy.service.message;

import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.metrics.NopLongHistogram;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LocalRemotingCommandTest {

    @Test
    public void testCreateRequestCommandShouldStartProcessTimer() {
        LocalRemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null, "JAVA");

        assertNotNull(command.getProcessTimer());
        assertTrue(command.getProcessTimer().isRunning());
    }

    @Test
    public void testWriteResponseShouldRecordLatencyForLocalRemotingCommand() {
        LocalRemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null, "JAVA");
        RemotingCommand response = RemotingCommand.createResponseCommand(0, null);
        AtomicBoolean latencyRecorded = new AtomicBoolean(false);
        LongHistogram rpcLatency = new NopLongHistogram() {
            @Override
            public void record(long value, Attributes attributes) {
                latencyRecorded.set(true);
            }
        };
        RemotingMetricsManager remotingMetricsManager = new RemotingMetricsManager() {
            @Override
            public LongHistogram getRpcLatency() {
                return rpcLatency;
            }
        };
        SimpleChannel channel = new SimpleChannel("127.0.0.1:8080", "127.0.0.1:8081") {
            @Override
            public ChannelFuture writeAndFlush(Object msg) {
                return new DefaultChannelPromise(this, ImmediateEventExecutor.INSTANCE).setSuccess();
            }
        };

        NettyRemotingAbstract.writeResponse(
            channel,
            command,
            response,
            null,
            remotingMetricsManager
        );

        assertTrue(latencyRecorded.get());
    }
}
