/**
 * $Id: ClientChannelInfo.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.client;

import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.LanguageCode;

import io.netty.channel.Channel;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class ClientChannelInfo {
    private final Channel channel;
    private final String clientId;
    private final LanguageCode language;
    private final int version;


    public ClientChannelInfo(Channel channel, String clientId, LanguageCode language, int version) {
        this.channel = channel;
        this.clientId = clientId;
        this.language = language;
        this.version = version;
    }


    public Channel getChannel() {
        return channel;
    }


    public String getClientId() {
        return clientId;
    }


    public LanguageCode getLanguage() {
        return language;
    }


    public int getVersion() {
        return version;
    }


    @Override
    public String toString() {
        return "ClientChannelInfo [channel=" + channel.remoteAddress() + ", clientId=" + clientId + ", language="
                + language + ", version=" + version + "]";
    }
}
