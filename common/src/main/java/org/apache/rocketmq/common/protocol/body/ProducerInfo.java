package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.LanguageCode;

import java.io.Serializable;

public class ProducerInfo implements Serializable {
    private String clientId;
    private String remoteIP;
    private LanguageCode language;
    private int version;
    private long lastUpdateTimestamp;

    public ProducerInfo(String clientId, String remoteIP, LanguageCode language, int version, long lastUpdateTimestamp) {
        this.clientId = clientId;
        this.remoteIP = remoteIP;
        this.language = language;
        this.version = version;
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    @Override
    public String toString() {
        return String.format("clientId=%s,remoteIP=%s, language=%s, version=%d, lastUpdateTimestamp=%d",
                clientId, remoteIP, language.name(), version, lastUpdateTimestamp);
    }
}
