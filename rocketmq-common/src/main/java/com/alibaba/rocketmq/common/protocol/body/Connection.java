package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.remoting.protocol.LanguageCode;


/**
 * TODO
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 13-8-5
 */
public class Connection {
    private String clientId;
    private String clientAddr;
    private LanguageCode language;
    private int version;


    public String getClientId() {
        return clientId;
    }


    public void setClientId(String clientId) {
        this.clientId = clientId;
    }


    public String getClientAddr() {
        return clientAddr;
    }


    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
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
}
