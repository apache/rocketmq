package org.apache.rocketmq.acl.common;

import org.apache.rocketmq.common.MixAll;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

public class SessionCredentials {
    public static final Charset CHARSET = Charset.forName("UTF-8");
    public static final String AccessKey = "AccessKey";
    public static final String SecretKey = "SecretKey";
    public static final String Signature = "Signature";
    public static final String SecurityToken = "SecurityToken";

    public static final String KeyFile = System.getProperty("rocketmq.client.keyFile",
        System.getProperty("user.home") + File.separator + "onskey");

    private String accessKey;
    private String secretKey;
    private String securityToken;
    private String signature;

    public SessionCredentials() {
        String keyContent = null;
        try {
            keyContent = MixAll.file2String(KeyFile);
        } catch (IOException ignore) {
        }
        if (keyContent != null) {
            Properties prop = MixAll.string2Properties(keyContent);
            if (prop != null) {
                this.updateContent(prop);
            }
        }
    }

    public SessionCredentials(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public SessionCredentials(String accessKey, String secretKey, String securityToken) {
        this(accessKey, secretKey);
        this.securityToken = securityToken;
    }


    public void updateContent(Properties prop) {
        {
            String value = prop.getProperty(AccessKey);
            if (value != null) {
                this.accessKey = value.trim();
            }
        }
        {
            String value = prop.getProperty(SecretKey);
            if (value != null) {
                this.secretKey = value.trim();
            }
        }
        {
            String value = prop.getProperty(SecurityToken);
            if (value != null) {
                this.securityToken = value.trim();
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

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getSecurityToken() {
        return securityToken;
    }

    public void setSecurityToken(final String securityToken) {
        this.securityToken = securityToken;
    }



    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((accessKey == null) ? 0 : accessKey.hashCode());
        result = prime * result + ((secretKey == null) ? 0 : secretKey.hashCode());
        result = prime * result + ((signature == null) ? 0 : signature.hashCode());
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
        } else if (!accessKey.equals(other.accessKey))
            return false;

        if (secretKey == null) {
            if (other.secretKey != null)
                return false;
        } else if (!secretKey.equals(other.secretKey))
            return false;

        if (signature == null) {
            if (other.signature != null)
                return false;
        } else if (!signature.equals(other.signature))
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "SessionCredentials [accessKey=" + accessKey + ", secretKey=" + secretKey + ", signature="
            + signature + ", SecurityToken=" + securityToken + "]";
    }
}