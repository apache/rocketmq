/**
 * $Id: ClientChannelInfo.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import com.alibaba.rocketmq.remoting.protocol.LanguageCode;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class ClientChannelInfo {
    private final Channel channel;
    private final String clientId;
    private final LanguageCode language;
    private final int version;
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();


    public ClientChannelInfo(Channel channel) {
        this(channel, null, null, 0);
    }


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


    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }


    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        result = prime * result + ((clientId == null) ? 0 : clientId.hashCode());
        result = prime * result + ((language == null) ? 0 : language.hashCode());
        result = prime * result + (int) (lastUpdateTimestamp ^ (lastUpdateTimestamp >>> 32));
        result = prime * result + version;
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClientChannelInfo other = (ClientChannelInfo) obj;
        if (channel == null) {
            if (other.channel != null)
                return false;
        }
        else if (this.channel != other.channel) {
            return false;
        }

        return true;
    }


    @Override
    public String toString() {
        return "ClientChannelInfo [channel=" + channel + ", clientId=" + clientId + ", language=" + language
                + ", version=" + version + ", lastUpdateTimestamp=" + lastUpdateTimestamp + "]";
    }
}
