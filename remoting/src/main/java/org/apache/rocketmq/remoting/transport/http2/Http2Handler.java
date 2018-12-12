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

package org.apache.rocketmq.remoting.transport.http2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2ConnectionDecoder;
import io.netty.handler.codec.http2.DefaultHttp2ConnectionEncoder;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.DefaultHttp2LocalFlowController;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.StreamBufferingEncoder;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http2.DefaultHttp2LocalFlowController.DEFAULT_WINDOW_UPDATE_RATIO;

public class Http2Handler extends Http2ConnectionHandler {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private boolean isServer;
    private int lastStreamId;

    private Http2Handler(final Http2ConnectionDecoder decoder, final Http2ConnectionEncoder encoder,
        final Http2Settings initialSettings, final boolean isServer) {
        super(decoder, encoder, initialSettings);
        decoder.frameListener(new FrameListener());
        this.isServer = isServer;
    }

    public static Http2Handler newHandler(final boolean isServer) {
        log.info("isServer: " + isServer);
        Http2HeadersDecoder headersDecoder = new DefaultHttp2HeadersDecoder(true);
        Http2FrameReader frameReader = new DefaultHttp2FrameReader(headersDecoder);
        Http2FrameWriter frameWriter = new DefaultHttp2FrameWriter();

        Http2Connection connection = new DefaultHttp2Connection(isServer);

        Http2ConnectionEncoder encoder = new StreamBufferingEncoder(
            new DefaultHttp2ConnectionEncoder(connection, frameWriter));

        connection.local().flowController(new DefaultHttp2LocalFlowController(connection,
            DEFAULT_WINDOW_UPDATE_RATIO, true));

        Http2ConnectionDecoder decoder = new DefaultHttp2ConnectionDecoder(connection, encoder,
            frameReader);

        Http2Settings settings = new Http2Settings();

        if (!isServer) {
            settings.pushEnabled(true);
        }

        settings.initialWindowSize(1048576 * 10); //10MiB
        settings.maxConcurrentStreams(Integer.MAX_VALUE);

        return newHandler(decoder, encoder, settings, isServer);
    }

    private static Http2Handler newHandler(final Http2ConnectionDecoder decoder, final Http2ConnectionEncoder encoder,
        final Http2Settings settings, boolean isServer) {
        return new Http2Handler(decoder, encoder, settings, isServer);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg,
        final ChannelPromise promise) throws Exception {
        if (isServer) {
            assert msg instanceof ByteBuf;
            sendAPushPromise(ctx, lastStreamId, lastStreamId + 1, (ByteBuf) msg);
        } else {

            final Http2Headers headers = new DefaultHttp2Headers();

            try {
                long threadId = Thread.currentThread().getId();
                long streamId = (threadId % 2 == 0) ? threadId + 1 : threadId + 2;
                encoder().writeHeaders(ctx, (int) streamId, headers, 0, false, promise);
                encoder().writeData(ctx, (int) streamId, (ByteBuf) msg, 0, false, ctx.newPromise());
                ctx.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    private void sendAPushPromise(ChannelHandlerContext ctx, int streamId, int pushPromiseStreamId,
        ByteBuf payload) throws Http2Exception {

        encoder().writePushPromise(ctx, streamId, pushPromiseStreamId,
            new DefaultHttp2Headers().status(OK.codeAsText()), 0, ctx.newPromise());

        Http2Headers headers = new DefaultHttp2Headers();
        headers.status(OK.codeAsText());
        encoder().writeHeaders(ctx, pushPromiseStreamId, headers, 0, false, ctx.newPromise());
        encoder().writeData(ctx, pushPromiseStreamId, payload, 0, false, ctx.newPromise());
    }

    private class FrameListener extends Http2FrameAdapter {
        @Override
        public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
            boolean endOfStream) throws Http2Exception {
            //Http2Handler.this.onDataRead(ctx, streamId, data, endOfStream);
            data.retain();
            Http2Handler.this.lastStreamId = streamId;
            ctx.fireChannelRead(data);
            return data.readableBytes() + padding;
        }
    }
}
