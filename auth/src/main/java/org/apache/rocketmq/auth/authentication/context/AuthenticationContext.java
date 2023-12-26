package org.apache.rocketmq.auth.authentication.context;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public abstract class AuthenticationContext {

    private String rpcCode;

    private Map<String, Object> extInfo;

    public String getRpcCode() {
        return rpcCode;
    }

    public void setRpcCode(String rpcCode) {
        this.rpcCode = rpcCode;
    }

    @SuppressWarnings("unchecked")
    public <T> T getExtInfo(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
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
        if (StringUtils.isBlank(key) || value == null) {
            return;
        }
        if (this.extInfo == null) {
            this.extInfo = new HashMap<>();
        }
        this.extInfo.put(key, value);
    }

    public boolean hasExtInfo(String key) {
        Object value = getExtInfo(key);
        return value != null;
    }

    public Map<String, Object> getExtInfo() {
        return extInfo;
    }

    public void setExtInfo(Map<String, Object> extInfo) {
        this.extInfo = extInfo;
    }
}
