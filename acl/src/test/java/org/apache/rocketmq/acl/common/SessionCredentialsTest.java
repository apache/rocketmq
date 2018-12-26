package org.apache.rocketmq.acl.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class SessionCredentialsTest {

    @Test
    public void equalsTest(){
        SessionCredentials sessionCredentials=new SessionCredentials("RocketMQ","12345678");
        sessionCredentials.setSecurityToken("abcd");
        SessionCredentials other=new SessionCredentials("RocketMQ","12345678","abcd");
        Assert.assertTrue(sessionCredentials.equals(other));
    }

    @Test
    public void updateContentTest(){
        SessionCredentials sessionCredentials=new SessionCredentials();
        Properties properties=new Properties();
        properties.setProperty(SessionCredentials.ACCESS_KEY,"RocketMQ");
        properties.setProperty(SessionCredentials.SECRET_KEY,"12345678");
        properties.setProperty(SessionCredentials.SECURITY_TOKEN,"abcd");
        sessionCredentials.updateContent(properties);
    }


}
