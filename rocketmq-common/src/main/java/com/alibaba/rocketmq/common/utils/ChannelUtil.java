package com.alibaba.rocketmq.common.utils;

import io.netty.channel.Channel;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * User: yubao.fyb
 * Date: 14/11/17
 * Time: 14:27
 */
public class ChannelUtil {
    public static String getRemoteIp(Channel channel) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.remoteAddress();
        if (inetSocketAddress == null) {
            return "";
        }
        final InetAddress inetAddr = inetSocketAddress.getAddress();
        return (inetAddr != null ? inetAddr.getHostAddress() : inetSocketAddress.getHostName());
    }

}
