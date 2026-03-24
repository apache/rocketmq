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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RemotingCodeDistributionHandlerTest {

    private NettyServerConfig nettyServerConfig;
    private RemotingCodeDistributionHandler handler;

    @Before
    public void setUp() {
        nettyServerConfig = new NettyServerConfig();
        handler = new RemotingCodeDistributionHandler(nettyServerConfig);
    }

    @Test
    public void testCountDistribution() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelPromise promise = mock(ChannelPromise.class);

        RemotingCommand inCmd = RemotingCommand.createRequestCommand(100, null);
        handler.channelRead(ctx, inCmd);
        handler.channelRead(ctx, inCmd);

        RemotingCommand outCmd = RemotingCommand.createResponseCommand(0, "ok");
        try {
            handler.write(ctx, outCmd, promise);
            handler.write(ctx, outCmd, promise);
            handler.write(ctx, outCmd, promise);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        Assert.assertEquals("{100:2}", handler.getInBoundSnapshotString());
        Assert.assertEquals("{0:3}", handler.getOutBoundSnapshotString());

        verify(ctx, times(2)).fireChannelRead(inCmd);
    }

    @Test
    public void testTrafficSizeWithBody() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        byte[] body = new byte[1024];
        RemotingCommand cmd = RemotingCommand.createRequestCommand(200, null);
        cmd.setBody(body);

        handler.channelRead(ctx, cmd);

        long expectedSize = RemotingCodeDistributionHandler.FIXED_OVERHEAD + 1024;
        Assert.assertEquals("{200:" + expectedSize + "}", handler.getInBoundTrafficSnapshotString());
    }

    @Test
    public void testTrafficSizeWithoutBody() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        RemotingCommand cmd = RemotingCommand.createRequestCommand(201, null);
        handler.channelRead(ctx, cmd);

        long expectedSize = RemotingCodeDistributionHandler.FIXED_OVERHEAD;
        Assert.assertEquals("{201:" + expectedSize + "}", handler.getInBoundTrafficSnapshotString());
    }

    @Test
    public void testDetailedSizeEnabled() {
        nettyServerConfig.setEnableDetailedTrafficSize(true);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        RemotingCommand cmd = RemotingCommand.createRequestCommand(300, null);
        cmd.setBody(new byte[512]);
        cmd.setRemark("test remark");
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        cmd.setExtFields(extFields);

        handler.channelRead(ctx, cmd);

        // FIXED_OVERHEAD + body(512)
        // + remark("test remark".length()=11)
        // + extField("topic"): keyLenPrefix(2) + "topic"(5) + valLenPrefix(4) + "TestTopic"(9) = 20
        // + extField("queueId"): keyLenPrefix(2) + "queueId"(7) + valLenPrefix(4) + "0"(1) = 14
        long expectedSize = RemotingCodeDistributionHandler.FIXED_OVERHEAD + 512 + 11 + 20 + 14;
        Assert.assertEquals("{300:" + expectedSize + "}", handler.getInBoundTrafficSnapshotString());
    }

    @Test
    public void testDetailedSizeDisabledIgnoresHeaderFields() {
        nettyServerConfig.setEnableDetailedTrafficSize(false);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        RemotingCommand cmd = RemotingCommand.createRequestCommand(301, null);
        cmd.setBody(new byte[256]);
        cmd.setRemark("some remark");
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("key", "value");
        cmd.setExtFields(extFields);

        handler.channelRead(ctx, cmd);

        // When disabled, only FIXED_OVERHEAD + body
        long expectedSize = RemotingCodeDistributionHandler.FIXED_OVERHEAD + 256;
        Assert.assertEquals("{301:" + expectedSize + "}", handler.getInBoundTrafficSnapshotString());
    }

    @Test
    public void testSnapshotResetsAfterRead() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        RemotingCommand cmd = RemotingCommand.createRequestCommand(400, null);
        handler.channelRead(ctx, cmd);

        // First read should have data
        Assert.assertNotNull(handler.getInBoundSnapshotString());
        Assert.assertNotNull(handler.getInBoundTrafficSnapshotString());

        // Second read should be null (reset by sumThenReset)
        Assert.assertNull(handler.getInBoundSnapshotString());
        Assert.assertNull(handler.getInBoundTrafficSnapshotString());
    }

    @Test
    public void testMultipleRequestCodes() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        RemotingCommand cmd1 = RemotingCommand.createRequestCommand(10, null);
        cmd1.setBody(new byte[100]);
        RemotingCommand cmd2 = RemotingCommand.createRequestCommand(20, null);
        cmd2.setBody(new byte[200]);

        handler.channelRead(ctx, cmd1);
        handler.channelRead(ctx, cmd1);
        handler.channelRead(ctx, cmd2);

        String countSnapshot = handler.getInBoundSnapshotString();
        Assert.assertNotNull(countSnapshot);
        Assert.assertTrue(countSnapshot.contains("10:2"));
        Assert.assertTrue(countSnapshot.contains("20:1"));

        // Traffic was already reset by getInBoundSnapshotString? No, they are separate maps.
        // Actually count and traffic are in the same TrafficStats object but
        // getCountSnapshot and getTrafficSnapshot reset independently
        String trafficSnapshot = handler.getInBoundTrafficSnapshotString();
        Assert.assertNotNull(trafficSnapshot);
        long size1 = RemotingCodeDistributionHandler.FIXED_OVERHEAD + 100;
        long size2 = RemotingCodeDistributionHandler.FIXED_OVERHEAD + 200;
        Assert.assertTrue(trafficSnapshot.contains("10:" + (size1 * 2)));
        Assert.assertTrue(trafficSnapshot.contains("20:" + size2));
    }

    @Test
    public void testNonRemotingCommandIgnored() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        handler.channelRead(ctx, "not a RemotingCommand");

        Assert.assertNull(handler.getInBoundSnapshotString());
        verify(ctx).fireChannelRead("not a RemotingCommand");
    }

    @Test
    public void testConcurrentAccess() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        int threadCount = 4;
        int countPerThread = 100_000;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicBoolean success = new AtomicBoolean(true);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        byte[] body = new byte[64];
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < countPerThread; j++) {
                        RemotingCommand cmd = RemotingCommand.createRequestCommand(1, null);
                        cmd.setBody(body);
                        handler.channelRead(ctx, cmd);
                    }
                } catch (Exception e) {
                    success.set(false);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        Assert.assertTrue(success.get());

        long totalCount = threadCount * (long) countPerThread;
        Assert.assertEquals("{1:" + totalCount + "}", handler.getInBoundSnapshotString());

        long expectedTotalTraffic = totalCount * (RemotingCodeDistributionHandler.FIXED_OVERHEAD + 64);
        Assert.assertEquals("{1:" + expectedTotalTraffic + "}", handler.getInBoundTrafficSnapshotString());

        executor.shutdown();
    }

    @Test
    public void testRuntimeSwitchToggle() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        RemotingCommand cmd = RemotingCommand.createRequestCommand(500, null);
        cmd.setBody(new byte[128]);
        cmd.setRemark("hello");

        // Record with detailed disabled
        nettyServerConfig.setEnableDetailedTrafficSize(false);
        handler.channelRead(ctx, cmd);
        long sizeOff = RemotingCodeDistributionHandler.FIXED_OVERHEAD + 128;
        Assert.assertEquals("{500:" + sizeOff + "}", handler.getInBoundTrafficSnapshotString());

        // Toggle on at runtime
        nettyServerConfig.setEnableDetailedTrafficSize(true);
        handler.channelRead(ctx, cmd);
        long sizeOn = RemotingCodeDistributionHandler.FIXED_OVERHEAD + 128 + 5; // + "hello".length()
        Assert.assertEquals("{500:" + sizeOn + "}", handler.getInBoundTrafficSnapshotString());
    }
}
