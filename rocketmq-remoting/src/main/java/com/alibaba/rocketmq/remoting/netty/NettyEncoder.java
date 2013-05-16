/**
 * $Id: NettyEncoder.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 *
 */
public class NettyEncoder extends MessageToByteEncoder<Object> {
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);


    @Override
    public void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        RemotingCommand cmd = null;
        try {
            if (msg instanceof RemotingCommand) {
                cmd = (RemotingCommand) msg;
                ByteBuffer header = cmd.encodeHeader();
                out.writeBytes(header);
                byte[] body = cmd.getBody();
                if (body != null) {
                    out.writeBytes(body);
                }
            }
        }
        catch (Exception e) {
            log.error("encode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            if (cmd != null) {
                log.error(cmd.toString());
            }

            ctx.channel().close();
        }
    }
}
