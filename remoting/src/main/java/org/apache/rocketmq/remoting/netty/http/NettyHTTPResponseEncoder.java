package org.apache.rocketmq.remoting.netty.http;

import java.util.List;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import com.alibaba.fastjson.JSON;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class NettyHTTPResponseEncoder extends HttpResponseEncoder {

	private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
	
    public boolean acceptOutboundMessage(Object msg) throws Exception {
        if (msg instanceof RemotingCommand) {
            return true;
        }
        return super.acceptOutboundMessage(msg);
    }

    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    	RemotingCommand remotingCommand = null;
    	try {
	        if (msg instanceof RemotingCommand) {
	            remotingCommand = (RemotingCommand) msg;
	            remotingCommand.makeCustomHeaderToNet();
	            byte[] bodyByte = remotingCommand.getBody();
	            if (bodyByte != null) {
	                ByteBuf bodyBuf = Base64.encode(Unpooled.buffer(bodyByte.length).setBytes(0, bodyByte).writerIndex(bodyByte.length));
	
	                remotingCommand.addExtField("beBody", new String(bodyBuf.array()));
	            }
	            byte[] contentByte = JSON.toJSONBytes(msg);
	            ByteBuf bytebuf = Unpooled.buffer(contentByte.length);
	            bytebuf.setBytes(0, contentByte);
	            bytebuf.writerIndex(contentByte.length);
	            DefaultFullHttpResponse defaultFullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, bytebuf);
	            defaultFullHttpResponse.headers().set("content-type", "application/json");
	            defaultFullHttpResponse.headers().set("Content-Length", contentByte.length);
	            super.encode(ctx, defaultFullHttpResponse, out);
	
	        } else if (msg instanceof FileRegion) {
	
	        }
    	}catch(Exception e) {
    		if(remotingCommand == null) {
    			RemotingUtil.closeChannel(ctx.channel());
    		}else {
    			RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "client decode exception");
    			response.setOpaque(remotingCommand.getOpaque());
    			ctx.channel().write(response);
    		}
    		log.error("broker encoder exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
    	}
    }
}
