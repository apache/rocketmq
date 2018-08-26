package org.apache.rocketmq.remoting.netty.http;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import com.alibaba.fastjson.JSON;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;

public class HttpClientHandler extends ChannelInboundHandlerAdapter  {

	private SimpleChannelInboundHandler<RemotingCommand> channelInboundHandler;
	
	public HttpClientHandler( SimpleChannelInboundHandler<RemotingCommand> channelInboundHandler) {
			this.channelInboundHandler = channelInboundHandler;
	}
	
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		ByteBuf buf = null;
		if (msg instanceof FullHttpResponse) {
			FullHttpResponse response = (FullHttpResponse) msg;
			buf = response.content();
		}else if(msg instanceof DefaultLastHttpContent) {
			DefaultLastHttpContent defaultLastHttpContent = (DefaultLastHttpContent)msg;
			buf = defaultLastHttpContent.content();
		}
		if(buf != null) {
			byte[] by = new byte[buf.writerIndex()];
			buf.getBytes(0, by);
			RemotingCommand remotingCommand = JSON.parseObject(by, RemotingCommand.class);
			this.channelInboundHandler.channelRead(ctx, remotingCommand);
			ctx.channel().closeFuture();
		}
	}
}
