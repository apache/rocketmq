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
package org.apache.rocketmq.remoting.netty.http;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import com.alibaba.fastjson.JSON;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;

public class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private SimpleChannelInboundHandler<RemotingCommand> channelInboundHandler;

    public HttpServerHandler(SimpleChannelInboundHandler<RemotingCommand> channelInboundHandler) {
        this.channelInboundHandler = channelInboundHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) {
        try {
            if (!fullHttpRequest.getDecoderResult().isSuccess()) {
                // sendError(channelHandlerContext, HttpResponseStatus.BAD_REQUEST);
                return;
            }

            if (fullHttpRequest.getMethod() == HttpMethod.GET) {
                // sendError(channelHandlerContext,HttpResponseStatus.METHOD_NOT_ALLOWED);
                return;
            }

            String uri = fullHttpRequest.getUri();
            ByteBuf buf = fullHttpRequest.content();
            byte[] by = new byte[buf.writerIndex()];
            buf.getBytes(0, by);
            RemotingCommand remotingCommand = JSON.parseObject(by, RemotingCommand.class);
            remotingCommand.setCode(URIUtils.getCode(uri));
            if (remotingCommand.getBody() == null && remotingCommand.getExtFields() != null) {
                String body = remotingCommand.getExtFields().remove("body");
                if (body != null) {
                    remotingCommand.setBody(body.getBytes());
                } else {
                    body = remotingCommand.getExtFields().remove("beBody");
                    if (body != null) {
                        byte[] bodyBy = body.getBytes();
                        ByteBuf bodyBuffer = Base64.decode(Unpooled.buffer(bodyBy.length).setBytes(0, bodyBy).writerIndex(bodyBy.length));
                        byte[] newBodyBy = new byte[bodyBuffer.writerIndex()];
                        bodyBuffer.getBytes(0, newBodyBy);
                        remotingCommand.setBody(newBodyBy);
                    }
                }
            }
            this.channelInboundHandler.channelRead(channelHandlerContext, remotingCommand);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
