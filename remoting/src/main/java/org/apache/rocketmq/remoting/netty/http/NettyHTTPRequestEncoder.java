package org.apache.rocketmq.remoting.netty.http;

import java.util.List;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import com.alibaba.fastjson.JSON;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpVersion;

public class NettyHTTPRequestEncoder extends HttpRequestEncoder {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    public boolean acceptOutboundMessage(Object msg) throws Exception {
        if (msg instanceof RemotingCommand) {
            return true;
        }
        return super.acceptOutboundMessage(msg);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
        try {
            if (msg instanceof RemotingCommand) {
                RemotingCommand remotingCommand = (RemotingCommand) msg;
                remotingCommand.makeCustomHeaderToNet();

                byte[] byteArray = remotingCommand.getBody();
                if (byteArray != null) {
                    ByteBuf bodyBuf = Base64.encode(Unpooled.buffer(byteArray.length).setBytes(0, byteArray).writerIndex(byteArray.length));
                    byte[] newByteArray = new byte[bodyBuf.writerIndex()];
                    bodyBuf.getBytes(0, newByteArray);
                    remotingCommand.addExtField("beBody", new String(newByteArray));
                }
                DefaultFullHttpRequest defaultFullHttpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                    HttpMethod.POST, URIUtils.getURI(remotingCommand.getCode()), Unpooled.wrappedBuffer(JSON.toJSONBytes(msg)));

                defaultFullHttpRequest.headers().set("content-type", "application/json");
                defaultFullHttpRequest.headers().set("Content-Length", defaultFullHttpRequest.content().readableBytes());
                super.encode(ctx, defaultFullHttpRequest, out);
            } else {
                super.encode(ctx, msg, out);
            }
        } catch (Exception e) {
            RemotingUtil.closeChannel(ctx.channel());
            log.error("NettyHTTPRequestEncoder Exception ", e);
        }
    }

}
