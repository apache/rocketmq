package org.apache.rocketmq.proxy.remoting.pipeline;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.Optional;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ContextInitPipeline implements RequestPipeline {

    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) throws Exception {
        Channel channel = ctx.channel();
        context.setAction(RemotingHelper.getRequestCodeDesc(request.getCode()))
            .setProtocolType(ChannelProtocolType.REMOTING.getName())
            .setChannel(channel)
            .setLocalAddress(NetworkUtil.socketAddress2String(ctx.channel().localAddress()))
            .setRemoteAddress(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        Optional.ofNullable(RemotingHelper.getAttributeValue(AttributeKeys.LANGUAGE_CODE_KEY, channel))
            .ifPresent(language -> context.setLanguage(language.name()));
        Optional.ofNullable(RemotingHelper.getAttributeValue(AttributeKeys.CLIENT_ID_KEY, channel))
            .ifPresent(context::setClientID);
        Optional.ofNullable(RemotingHelper.getAttributeValue(AttributeKeys.VERSION_KEY, channel))
            .ifPresent(version -> context.setClientVersion(MQVersion.getVersionDesc(version)));
    }
}
