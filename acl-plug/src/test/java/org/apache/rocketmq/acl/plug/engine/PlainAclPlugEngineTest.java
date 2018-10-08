package org.apache.rocketmq.acl.plug.engine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControl;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControlTransport;
import org.apache.rocketmq.acl.plug.entity.ControllerParametersEntity;
import org.apache.rocketmq.acl.plug.entity.LoginInfo;
import org.apache.rocketmq.acl.plug.entity.LoginOrRequestAccessControl;
import org.apache.rocketmq.acl.plug.exception.AclPlugAccountAnalysisException;
import org.apache.rocketmq.common.MixAll;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.junit.MockitoJUnitRunner;
import org.yaml.snakeyaml.Yaml;

@RunWith(MockitoJUnitRunner.class)
public class PlainAclPlugEngineTest {

    PlainAclPlugEngine plainAclPlugEngine;

    BorkerAccessControlTransport transport;

    AccessControl accessControl;

    AccessControl accessControlTwo;

    Map<String, LoginInfo> loginInfoMap;

    @Before
    public void init() throws FileNotFoundException, NoSuchFieldException, SecurityException {
        String home = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
        Yaml ymal = new Yaml();
        String filePath = home + "/conf/transport.yml";
        FileInputStream fis = new FileInputStream(new File(filePath));
        transport = ymal.loadAs(fis, BorkerAccessControlTransport.class);

        ControllerParametersEntity controllerParametersEntity = new ControllerParametersEntity();
        controllerParametersEntity.setFileHome(home);
        plainAclPlugEngine = new PlainAclPlugEngine(controllerParametersEntity);

        accessControl = new BorkerAccessControl();
        accessControl.setAccount("rokcetmq");
        accessControl.setPassword("aliyun");
        accessControl.setNetaddress("127.0.0.1");
        accessControl.setRecognition("127.0.0.1:1");

        accessControlTwo = new BorkerAccessControl();
        accessControlTwo.setAccount("rokcet");
        accessControlTwo.setPassword("aliyun");
        accessControlTwo.setNetaddress("127.0.0.1");
        accessControlTwo.setRecognition("127.0.0.1:2");

        loginInfoMap = new ConcurrentHashMap<>();
        FieldSetter.setField(plainAclPlugEngine, plainAclPlugEngine.getClass().getSuperclass().getDeclaredField("loginInfoMap"), loginInfoMap);

    }

    @Test(expected = AclPlugAccountAnalysisException.class)
    public void testPlainAclPlugEngineInit() {
        ControllerParametersEntity controllerParametersEntity = new ControllerParametersEntity();
        new PlainAclPlugEngine(controllerParametersEntity);

    }

    @Test
    public void authenticationInfoOfSetAccessControl() {
        AuthenticationInfoManagementAclPlugEngine aclPlugEngine = (AuthenticationInfoManagementAclPlugEngine) plainAclPlugEngine;
        aclPlugEngine.setAccessControl(accessControl);

        AuthenticationInfo authenticationInfo = aclPlugEngine.getAccessControl(accessControl);

        AccessControl getAccessControl = authenticationInfo.getAccessControl();
        Assert.assertEquals(accessControl, getAccessControl);

        AccessControl testAccessControl = new AccessControl();
        testAccessControl.setAccount("rokcetmq");
        testAccessControl.setPassword("aliyun");
        testAccessControl.setNetaddress("127.0.0.1");
        testAccessControl.setRecognition("127.0.0.1:1");

        testAccessControl.setAccount("rokcetmq1");
        authenticationInfo = aclPlugEngine.getAccessControl(testAccessControl);
        Assert.assertNull(authenticationInfo);

        testAccessControl.setAccount("rokcetmq");
        testAccessControl.setPassword("1");
        authenticationInfo = aclPlugEngine.getAccessControl(testAccessControl);
        Assert.assertNull(authenticationInfo);

        testAccessControl.setNetaddress("127.0.0.2");
        authenticationInfo = aclPlugEngine.getAccessControl(testAccessControl);
        Assert.assertNull(authenticationInfo);
    }

    @Test
    public void setAccessControlList() {
        List<AccessControl> accessControlList = new ArrayList<>();
        accessControlList.add(accessControl);

        accessControlList.add(accessControlTwo);

        plainAclPlugEngine.setAccessControlList(accessControlList);

        AuthenticationInfoManagementAclPlugEngine aclPlugEngine = (AuthenticationInfoManagementAclPlugEngine) plainAclPlugEngine;
        AuthenticationInfo newAccessControl = aclPlugEngine.getAccessControl(accessControl);
        Assert.assertEquals(accessControl, newAccessControl.getAccessControl());

        newAccessControl = aclPlugEngine.getAccessControl(accessControlTwo);
        Assert.assertEquals(accessControlTwo, newAccessControl.getAccessControl());

    }

    @Test
    public void setNetaddressAccessControl() {
        AuthenticationInfoManagementAclPlugEngine aclPlugEngine = (AuthenticationInfoManagementAclPlugEngine) plainAclPlugEngine;
        AccessControl accessControl = new BorkerAccessControl();
        accessControl.setNetaddress("127.0.0.1");
        aclPlugEngine.setAccessControl(accessControl);
        aclPlugEngine.setNetaddressAccessControl(accessControl);

        AuthenticationInfo authenticationInfo = aclPlugEngine.getAccessControl(accessControl);

        AccessControl getAccessControl = authenticationInfo.getAccessControl();
        Assert.assertEquals(accessControl, getAccessControl);

        accessControl.setNetaddress("127.0.0.2");
        authenticationInfo = aclPlugEngine.getAccessControl(accessControl);
        Assert.assertNull(authenticationInfo);
    }

    public void eachCheckLoginAndAuthentication() {

    }

    @Test(expected = AclPlugAccountAnalysisException.class)
    public void borkerAccessControlTransportTestNull() {
        plainAclPlugEngine.setBorkerAccessControlTransport(new BorkerAccessControlTransport());
    }

    @Test
    public void borkerAccessControlTransportTest() {
        BorkerAccessControlTransport borkerAccessControlTransprt = new BorkerAccessControlTransport();
        borkerAccessControlTransprt.setOnlyNetAddress((BorkerAccessControl) this.accessControl);
        List<BorkerAccessControl> list = new ArrayList<>();
        list.add((BorkerAccessControl) this.accessControlTwo);
        borkerAccessControlTransprt.setList(list);
        plainAclPlugEngine.setBorkerAccessControlTransport(borkerAccessControlTransprt);

        AuthenticationInfoManagementAclPlugEngine aclPlugEngine = (AuthenticationInfoManagementAclPlugEngine) plainAclPlugEngine;
        AccessControl accessControl = new BorkerAccessControl();
        accessControl.setNetaddress("127.0.0.1");
        aclPlugEngine.setAccessControl(accessControl);
        AuthenticationInfo authenticationInfo = aclPlugEngine.getAccessControl(accessControl);
        Assert.assertNotNull(authenticationInfo.getAccessControl());

        authenticationInfo = aclPlugEngine.getAccessControl(accessControlTwo);
        Assert.assertEquals(accessControlTwo, authenticationInfo.getAccessControl());

    }

    @Test
    public void getLoginInfo() {
        plainAclPlugEngine.setAccessControl(accessControl);
        LoginInfo loginInfo = plainAclPlugEngine.getLoginInfo(accessControl);
        Assert.assertNotNull(loginInfo);

        loginInfo = plainAclPlugEngine.getLoginInfo(accessControlTwo);
        Assert.assertNull(loginInfo);

    }

    @Test
    public void deleteLoginInfo() {
        plainAclPlugEngine.setAccessControl(accessControl);
        plainAclPlugEngine.getLoginInfo(accessControl);

        LoginInfo loginInfo = loginInfoMap.get(accessControl.getRecognition());
        Assert.assertNotNull(loginInfo);

        plainAclPlugEngine.deleteLoginInfo(accessControl.getRecognition());

        loginInfo = loginInfoMap.get(accessControl.getRecognition());
        Assert.assertNull(loginInfo);
    }

    @Test
    public void getAuthenticationInfo() {
        LoginOrRequestAccessControl loginOrRequestAccessControl = new LoginOrRequestAccessControl();
        loginOrRequestAccessControl.setAccount("rokcetmq");
        loginOrRequestAccessControl.setPassword("aliyun");
        loginOrRequestAccessControl.setNetaddress("127.0.0.1");
        loginOrRequestAccessControl.setRecognition("127.0.0.1:1");

        AuthenticationResult authenticationResult = new AuthenticationResult();
        plainAclPlugEngine.getAuthenticationInfo(loginOrRequestAccessControl, authenticationResult);
        Assert.assertEquals("Login information does not exist, Please check login, password, IP", authenticationResult.getResultString());

        plainAclPlugEngine.setAccessControl(accessControl);
        AuthenticationInfo authenticationInfo = plainAclPlugEngine.getAuthenticationInfo(loginOrRequestAccessControl, authenticationResult);
        Assert.assertNotNull(authenticationInfo);

    }
}
