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
				/*String customHeader = remotingCommand.getExtFields().remove("customHeader");
				if(customHeader != null) {
					remotingCommand.writeCustomHeader(JSON.);
				}*/
            }

            this.channelInboundHandler.channelRead(channelHandlerContext, remotingCommand);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
