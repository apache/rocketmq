package org.apache.rocketmq.acl.plug;

import java.util.HashMap;

import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControl;
import org.apache.rocketmq.acl.plug.exception.AclPlugRuntimeException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;;

public class AclRemotingServiceTest {


    AclRemotingService defaultAclService;

    AccessValidator accessValidator;

    AccessControl accessControl;

    AccessControl accessControlTwo;

    @Before
    public void init() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        DefaultAclRemotingServiceImpl aclRemotingServiceImpl = new DefaultAclRemotingServiceImpl();
        defaultAclService = aclRemotingServiceImpl;
        accessValidator = aclRemotingServiceImpl;

        accessControl = new BorkerAccessControl();
        accessControl.setAccount("RocketMQ");
        accessControl.setPassword("1234567");
        accessControl.setNetaddress("192.0.0.1");
        accessControl.setRecognition("127.0.0.1:1");

        accessControlTwo = new BorkerAccessControl();
        accessControlTwo.setAccount("RocketMQ");
        accessControlTwo.setPassword("1234567");
        accessControlTwo.setNetaddress("192.0.2.1");
        accessControlTwo.setRecognition("127.0.0.1:2");
    }


    @Test
    public void defaultConstructorTest() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        AclRemotingService defaultAclService = new DefaultAclRemotingServiceImpl();
        Assert.assertNotNull(defaultAclService);
    }

    @Test
    public void parseTest() {
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(34, "");
        HashMap<String, String> map = new HashMap<>();
        map.put("account", "RocketMQ");
        map.put("password", "123456");
        map.put("topic", "test");
        remotingCommand.setExtFields(map);

        AccessResource accessResource = accessValidator.parse(remotingCommand, "127.0.0.1:123");
        AccessControl accessControl = (AccessControl) accessResource;
        AccessControl newAccessControl = new AccessControl();
        newAccessControl.setAccount("RocketMQ");
        newAccessControl.setPassword("123456");
        newAccessControl.setTopic("test");
        newAccessControl.setCode(34);
        newAccessControl.setNetaddress("127.0.0.1");
        newAccessControl.setRecognition("127.0.0.1:123");
        Assert.assertEquals(accessControl.toString(), newAccessControl.toString());
    }

    @Test
    public void checkTest() {
        accessControl.setCode(34);
        AuthenticationResult authenticationResult = defaultAclService.check(accessControl);
        Assert.assertTrue(authenticationResult.isSucceed());
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void checkAccessExceptionTest() {
        accessControl.setCode(34);
        accessControl.setAccount("Rocketmq");
        defaultAclService.check(accessControl);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void checkPasswordTest() {
        accessControl.setCode(34);
        accessControl.setPassword("123123123");
        defaultAclService.check(accessControl);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void checkCodeTest() {
        accessControl.setCode(14434);
        accessControl.setPassword("123123123");
        defaultAclService.check(accessControl);
    }


    @Test
    public void validateTest() {
        accessControl.setCode(34);
        accessValidator.validate(accessControl);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void validateAccessExceptionTest() {
        accessControl.setCode(34);
        accessControl.setAccount("Rocketmq");
        accessValidator.validate(accessControl);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void validatePasswordTest() {
        accessControl.setCode(34);
        accessControl.setPassword("123123123");
        accessValidator.validate(accessControl);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void validateCodeTest() {
        accessControl.setCode(14434);
        accessControl.setPassword("123123123");
        accessValidator.validate(accessControl);
    }
}
