package com.alibaba.rocketmq.common;

import java.io.File;
import java.util.Properties;


public class SessionCredentials {
    public static final String AccessKey = "AccessKey";
    public static final String SecretKey = "SecretKey";

    private String accessKey;
    private String secretKey;


    public SessionCredentials() {
        final String keyFile = System.getProperty("user.home") + File.separator + "onskey";
        final String keyContent = MixAll.file2String(keyFile);
        if (keyContent != null) {
            Properties prop = MixAll.string2Properties(keyContent);
            if (prop != null) {
                this.updateContent(prop);
            }
        }
    }


    public void updateContent(Properties prop) {
        {
            String value = prop.getProperty("AccessKey");
            if (value != null) {
                this.accessKey = value;
            }
        }
        {
            String value = prop.getProperty("SecretKey");
            if (value != null) {
                this.secretKey = value;
            }
        }
    }


    public String getAccessKey() {
        return accessKey;
    }


    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }


    public String getSecretKey() {
        return secretKey;
    }


    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((accessKey == null) ? 0 : accessKey.hashCode());
        result = prime * result + ((secretKey == null) ? 0 : secretKey.hashCode());
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
        SessionCredentials other = (SessionCredentials) obj;
        if (accessKey == null) {
            if (other.accessKey != null)
                return false;
        }
        else if (!accessKey.equals(other.accessKey))
            return false;
        if (secretKey == null) {
            if (other.secretKey != null)
                return false;
        }
        else if (!secretKey.equals(other.secretKey))
            return false;
        return true;
    }
}
