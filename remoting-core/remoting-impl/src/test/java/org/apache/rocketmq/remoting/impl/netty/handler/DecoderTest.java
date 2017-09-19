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

package org.apache.rocketmq.remoting.impl.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.api.protocol.Protocol;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class DecoderTest {

    private EmbeddedChannel channel;
    private AtomicInteger messagesReceived;
    private AtomicInteger exceptionsCaught;

    @Before
    public void setUp() {
        exceptionsCaught = new AtomicInteger(0);
        messagesReceived = new AtomicInteger(0);
        Decoder decoder = new Decoder();

        channel = new EmbeddedChannel(decoder, new SimpleChannelInboundHandler<Object>() {

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
                messagesReceived.incrementAndGet();
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
                exceptionsCaught.incrementAndGet();
            }
        });
    }

    @After
    public void tearDown() {
        channel.close();
    }

    @Test
    public void decodeEmptyBufferTest() throws Exception {
        // Send an empty buffer and make sure nothing breaks
        channel.writeInbound(Unpooled.EMPTY_BUFFER);
        channel.finish();

        Assert.assertEquals(exceptionsCaught.get(), 0);
        Assert.assertEquals(messagesReceived.get(), 0);
    }

    @Test
    public void decodeHalfMessageTest() throws Exception {
        //Case 1
        ByteBuf buf = Unpooled.buffer().writeBytes(new byte[] {'a', 'b', 'c'});

        channel.writeInbound(buf.duplicate());
        channel.finish();

        Assert.assertEquals(exceptionsCaught.get(), 0);
        Assert.assertEquals(messagesReceived.get(), 0);

        //Case 2
        buf = Unpooled.buffer();
        setUp();

        buf.writeByte(Protocol.HTTP_2_MAGIC);
        buf.writeInt(22);

        channel.writeInbound(buf.duplicate());
        TimeUnit.MILLISECONDS.sleep(100);

        buf = Unpooled.buffer();
        buf.writeInt(12);
        buf.writeByte(1);
        buf.writeByte(1);
        buf.writeShort(2);
        buf.writeBytes(new byte[] {'c', 'd'});
        buf.writeShort(2);
        buf.writeBytes(new byte[] {'e', 'f'});
        buf.writeShort(0);
        buf.writeInt(2);
        buf.writeBytes(new byte[] {'g', 'h'});

        channel.writeInbound(buf.duplicate());
        channel.finish();

        Assert.assertEquals(exceptionsCaught.get(), 0);
        Assert.assertEquals(messagesReceived.get(), 1);
    }

    @Test
    public void decodeIllegalLengthTest() throws Exception {
        ByteBuf buf = Unpooled.buffer();

        buf.writeByte(Protocol.HTTP_2_MAGIC);
        buf.writeInt(0);

        buf.writeInt(12);
        buf.writeByte(1);
        buf.writeByte(1);
        buf.writeBytes(new byte[] {'a', 'b'});
        buf.writeBytes(new byte[] {'c', 'd'});
        buf.writeBytes(new byte[] {'e', 'f'});
        buf.writeInt(15);

        channel.writeInbound(buf.duplicate());
        channel.finish();

        Assert.assertEquals(exceptionsCaught.get(), 1);
    }

    @Test
    public void decodeTest() throws Exception {
        ByteBuf buf = Unpooled.buffer();

        buf.writeByte(Protocol.HTTP_2_MAGIC);
        buf.writeInt(22);

        buf.writeInt(12);
        buf.writeByte(1);
        buf.writeByte(1);
        buf.writeShort(2);
        buf.writeBytes(new byte[] {'c', 'd'});
        buf.writeShort(2);
        buf.writeBytes(new byte[] {'e', 'f'});
        buf.writeShort(0);
        buf.writeInt(2);
        buf.writeBytes(new byte[] {'g', 'h'});

        channel.writeInbound(buf.duplicate());
        channel.finish();

        Assert.assertEquals(exceptionsCaught.get(), 0);
        Assert.assertEquals(messagesReceived.get(), 1);
    }
}