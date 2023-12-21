package org.apache.rocketmq.auth.authentication.context;

import java.util.HashMap;
import java.util.Map;

public class DefaultAuthenticationContext implements AuthenticationContext {

    private String username;

    private byte[] content;

    private String signature;

    private Map<String, Object> extInfo;

    @SuppressWarnings("unchecked")
    public <T> T getExtInfo(String key) {
        if (this.extInfo == null) {
            return null;
        }
        Object value = this.extInfo.get(key);
        if (value == null) {
            return null;
        }
        return (T) value;
    }

    public void setExtInfo(String key, Object value) {
        if (this.extInfo == null) {
            this.extInfo = new HashMap<>();
        }
        this.extInfo.put(key, value);
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public Map<String, Object> getExtInfo() {
        return extInfo;
    }

    public void setExtInfo(Map<String, Object> extInfo) {
        this.extInfo = extInfo;
    }
}
