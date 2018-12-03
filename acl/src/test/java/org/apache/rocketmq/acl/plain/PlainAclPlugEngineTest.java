/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.acl.plain;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.rocketmq.acl.plain.PlainPermissionLoader.AccessContralAnalysis;
import org.apache.rocketmq.acl.plain.PlainPermissionLoader.BrokerAccessControlTransport;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PlainAclPlugEngineTest {

    PlainPermissionLoader plainPermissionLoader;

    AccessContralAnalysis accessContralAnalysis = new AccessContralAnalysis();

    PlainAccessResource plainAccessResource;

    PlainAccessResource plainAccessResourceTwo;

    AuthenticationInfo authenticationInfo;

    BrokerAccessControl brokerAccessControl;

    Set<Integer> adminCode = new HashSet<>();

    @Before
    public void init() throws NoSuchFieldException, SecurityException, IOException {
        //  UPDATE_AND_CREATE_TOPIC
        adminCode.add(17);
        //  UPDATE_BROKER_CONFIG
        adminCode.add(25);
        //  DELETE_TOPIC_IN_BROKER
        adminCode.add(215);
        // UPDATE_AND_CREATE_SUBSCRIPTIONGROUP
        adminCode.add(200);
        // DELETE_SUBSCRIPTIONGROUP
        adminCode.add(207);

        accessContralAnalysis.analysisClass(RequestCode.class);

        brokerAccessControl = new BrokerAccessControl();
        // 321
        brokerAccessControl.setQueryConsumeQueue(false);

        Set<String> permitSendTopic = new HashSet<>();
        permitSendTopic.add("permitSendTopic");
        brokerAccessControl.setPermitSendTopic(permitSendTopic);

        Set<String> noPermitSendTopic = new HashSet<>();
        noPermitSendTopic.add("noPermitSendTopic");
        brokerAccessControl.setNoPermitSendTopic(noPermitSendTopic);

        Set<String> permitPullTopic = new HashSet<>();
        permitPullTopic.add("permitPullTopic");
        brokerAccessControl.setPermitPullTopic(permitPullTopic);

        Set<String> noPermitPullTopic = new HashSet<>();
        noPermitPullTopic.add("noPermitPullTopic");
        brokerAccessControl.setNoPermitPullTopic(noPermitPullTopic);

        AccessContralAnalysis accessContralAnalysis = new AccessContralAnalysis();
        accessContralAnalysis.analysisClass(RequestCode.class);
        Map<Integer, Boolean> map = accessContralAnalysis.analysis(brokerAccessControl);

        authenticationInfo = new AuthenticationInfo(map, brokerAccessControl, RemoteAddressStrategyFactory.NULL_NET_ADDRESS_STRATEGY);

        System.setProperty("rocketmq.home.dir", "src/test/resources");
        plainPermissionLoader = new PlainPermissionLoader();

        plainAccessResource = new BrokerAccessControl();
        plainAccessResource.setAccessKey("rokcetmq");
        plainAccessResource.setSignature("aliyun11");
        plainAccessResource.setRemoteAddr("127.0.0.1");
        plainAccessResource.setRecognition("127.0.0.1:1");

        plainAccessResourceTwo = new BrokerAccessControl();
        plainAccessResourceTwo.setAccessKey("rokcet1");
        plainAccessResourceTwo.setSignature("aliyun1");
        plainAccessResourceTwo.setRemoteAddr("127.0.0.1");
        plainAccessResourceTwo.setRecognition("127.0.0.1:2");

    }

    @Test(expected = AclPlugRuntimeException.class)
    public void accountNullTest() {
        plainAccessResource.setAccessKey(null);
        plainPermissionLoader.setAccessControl(plainAccessResource);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void accountThanTest() {
        plainAccessResource.setAccessKey("123");
        plainPermissionLoader.setAccessControl(plainAccessResource);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void passWordtNullTest() {
        plainAccessResource.setAccessKey(null);
        plainPermissionLoader.setAccessControl(plainAccessResource);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void passWordThanTest() {
        plainAccessResource.setAccessKey("123");
        plainPermissionLoader.setAccessControl(plainAccessResource);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void testPlainAclPlugEngineInit() {
        System.setProperty("rocketmq.home.dir", "");
        new PlainPermissionLoader().initialize();
    }

    @Test
    public void authenticationInfoOfSetAccessControl() {
        plainPermissionLoader.setAccessControl(plainAccessResource);

        AuthenticationInfo authenticationInfo = plainPermissionLoader.getAccessControl(plainAccessResource);

        PlainAccessResource getPlainAccessResource = authenticationInfo.getPlainAccessResource();
        Assert.assertEquals(plainAccessResource, getPlainAccessResource);

        PlainAccessResource testPlainAccessResource = new PlainAccessResource();
        testPlainAccessResource.setAccessKey("rokcetmq");
        testPlainAccessResource.setSignature("aliyun11");
        testPlainAccessResource.setRemoteAddr("127.0.0.1");
        testPlainAccessResource.setRecognition("127.0.0.1:1");

        testPlainAccessResource.setAccessKey("rokcetmq1");
        authenticationInfo = plainPermissionLoader.getAccessControl(testPlainAccessResource);
        Assert.assertNull(authenticationInfo);

        testPlainAccessResource.setAccessKey("rokcetmq");
        testPlainAccessResource.setSignature("1234567");
        authenticationInfo = plainPermissionLoader.getAccessControl(testPlainAccessResource);
        Assert.assertNull(authenticationInfo);

        testPlainAccessResource.setRemoteAddr("127.0.0.2");
        authenticationInfo = plainPermissionLoader.getAccessControl(testPlainAccessResource);
        Assert.assertNull(authenticationInfo);
    }

    @Test
    public void setAccessControlList() {
        List<PlainAccessResource> plainAccessResourceList = new ArrayList<>();
        plainAccessResourceList.add(plainAccessResource);

        plainAccessResourceList.add(plainAccessResourceTwo);

        plainPermissionLoader.setAccessControlList(plainAccessResourceList);

        AuthenticationInfo newAccessControl = plainPermissionLoader.getAccessControl(plainAccessResource);
        Assert.assertEquals(plainAccessResource, newAccessControl.getPlainAccessResource());

        newAccessControl = plainPermissionLoader.getAccessControl(plainAccessResourceTwo);
        Assert.assertEquals(plainAccessResourceTwo, newAccessControl.getPlainAccessResource());

    }

    @Test
    public void setNetaddressAccessControl() {
        PlainAccessResource plainAccessResource = new BrokerAccessControl();
        plainAccessResource.setAccessKey("RocketMQ");
        plainAccessResource.setSignature("RocketMQ");
        plainAccessResource.setRemoteAddr("127.0.0.1");
        plainPermissionLoader.setAccessControl(plainAccessResource);
        plainPermissionLoader.setNetaddressAccessControl(plainAccessResource);

        AuthenticationInfo authenticationInfo = plainPermissionLoader.getAccessControl(plainAccessResource);

        PlainAccessResource getPlainAccessResource = authenticationInfo.getPlainAccessResource();
        Assert.assertEquals(plainAccessResource, getPlainAccessResource);

        plainAccessResource.setRemoteAddr("127.0.0.2");
        authenticationInfo = plainPermissionLoader.getAccessControl(plainAccessResource);
        Assert.assertNull(authenticationInfo);
    }

    public void eachCheckLoginAndAuthentication() {

    }

    @Test(expected = AclPlugRuntimeException.class)
    public void BrokerAccessControlTransportTestNull() {
        BrokerAccessControlTransport accessControlTransport = new BrokerAccessControlTransport();
        plainPermissionLoader.setBrokerAccessControlTransport(accessControlTransport);
    }

    @Test
    public void BrokerAccessControlTransportTest() {
        BrokerAccessControlTransport accessControlTransport = new BrokerAccessControlTransport();
        List<BrokerAccessControl> list = new ArrayList<>();
        list.add((BrokerAccessControl) this.plainAccessResourceTwo);
        accessControlTransport.setOnlyNetAddress((BrokerAccessControl) this.plainAccessResource);
        accessControlTransport.setList(list);
        plainPermissionLoader.setBrokerAccessControlTransport(accessControlTransport);

        PlainAccessResource plainAccessResource = new BrokerAccessControl();
        plainAccessResource.setAccessKey("RocketMQ");
        plainAccessResource.setSignature("RocketMQ");
        plainAccessResource.setRemoteAddr("127.0.0.1");
        plainPermissionLoader.setAccessControl(plainAccessResource);
        AuthenticationInfo authenticationInfo = plainPermissionLoader.getAccessControl(plainAccessResource);
        Assert.assertNotNull(authenticationInfo.getPlainAccessResource());

        authenticationInfo = plainPermissionLoader.getAccessControl(plainAccessResourceTwo);
        Assert.assertEquals(plainAccessResourceTwo, authenticationInfo.getPlainAccessResource());

    }

    @Test
    public void authenticationTest() {
        AuthenticationResult authenticationResult = new AuthenticationResult();
        plainAccessResource.setRequestCode(317);

        boolean isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setRequestCode(321);
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        plainAccessResource.setRequestCode(10);
        plainAccessResource.setTopic("permitSendTopic");
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setRequestCode(310);
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setRequestCode(320);
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setTopic("noPermitSendTopic");
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        plainAccessResource.setTopic("nopermitSendTopic");
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        plainAccessResource.setRequestCode(11);
        plainAccessResource.setTopic("permitPullTopic");
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setTopic("noPermitPullTopic");
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        plainAccessResource.setTopic("nopermitPullTopic");
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

    }

    @Test
    public void isEmptyTest() {
        AuthenticationResult authenticationResult = new AuthenticationResult();
        plainAccessResource.setRequestCode(10);
        plainAccessResource.setTopic("absentTopic");
        boolean isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        Set<String> permitSendTopic = new HashSet<>();
        brokerAccessControl.setPermitSendTopic(permitSendTopic);
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setRequestCode(11);
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        brokerAccessControl.setPermitPullTopic(permitSendTopic);
        isReturn = plainPermissionLoader.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);
    }

    @Test
    public void adminBrokerAccessControlTest() {
        BrokerAccessControl admin = new BrokerAccessControl();
        admin.setAccessKey("adminTest");
        admin.setSignature("adminTest");
        admin.setRemoteAddr("127.0.0.1");
        plainPermissionLoader.setAccessControl(admin);
        Assert.assertFalse(admin.isUpdateAndCreateTopic());

        admin.setAdmin(true);
        plainPermissionLoader.setAccessControl(admin);
        Assert.assertTrue(admin.isUpdateAndCreateTopic());
    }

    @Test
    public void adminEachCheckAuthentication() {
        BrokerAccessControl accessControl = new BrokerAccessControl();
        accessControl.setAccessKey("RocketMQ1");
        accessControl.setSignature("1234567");
        accessControl.setRemoteAddr("127.0.0.1");
        plainPermissionLoader.setAccessControl(accessControl);
        for (Integer code : adminCode) {
            accessControl.setRequestCode(code);
            AuthenticationResult authenticationResult = plainPermissionLoader.eachCheckAuthentication(accessControl);
            Assert.assertFalse(authenticationResult.isSucceed());

        }
        plainPermissionLoader.cleanAuthenticationInfo();
        accessControl.setAdmin(true);
        plainPermissionLoader.setAccessControl(accessControl);
        for (Integer code : adminCode) {
            accessControl.setRequestCode(code);
            AuthenticationResult authenticationResult = plainPermissionLoader.eachCheckAuthentication(accessControl);
            Assert.assertTrue(authenticationResult.isSucceed());
        }
    }

    @Test
    public void cleanAuthenticationInfoTest() {
        plainPermissionLoader.setAccessControl(plainAccessResource);
        plainAccessResource.setRequestCode(202);
        AuthenticationResult authenticationResult = plainPermissionLoader.eachCheckAuthentication(plainAccessResource);
        Assert.assertTrue(authenticationResult.isSucceed());
        plainPermissionLoader.cleanAuthenticationInfo();
        authenticationResult = plainPermissionLoader.eachCheckAuthentication(plainAccessResource);
        Assert.assertFalse(authenticationResult.isSucceed());
    }

    @Test
    public void isWatchStartTest() {
        PlainPermissionLoader plainPermissionLoader = new PlainPermissionLoader();
        Assert.assertTrue(plainPermissionLoader.isWatchStart());
        System.setProperty("java.version", "1.6.11");
        plainPermissionLoader = new PlainPermissionLoader();
        Assert.assertFalse(plainPermissionLoader.isWatchStart());
    }

    @Test
    public void watchTest() throws IOException {
        System.setProperty("rocketmq.home.dir", "src/test/resources/watch");
        File file = new File("src/test/resources/watch/conf");
        file.mkdirs();
        File transport = new File("src/test/resources/watch/conf/transport.yml");
        transport.createNewFile();

        FileWriter writer = new FileWriter(transport);
        writer.write("list:\r\n");
        writer.write("- account: rokcetmq\r\n");
        writer.write("  password: aliyun11\r\n");
        writer.write("  netaddress: 127.0.0.1\r\n");
        writer.flush();
        writer.close();
        PlainPermissionLoader plainPermissionLoader = new PlainPermissionLoader();
        plainAccessResource.setRequestCode(203);
        AuthenticationResult authenticationResult = plainPermissionLoader.eachCheckAuthentication(plainAccessResource);
        Assert.assertTrue(authenticationResult.isSucceed());

        writer = new FileWriter(new File("src/test/resources/watch/conf/transport.yml"), true);
        writer.write("- account: rokcet1\r\n");
        writer.write("  password: aliyun1\r\n");
        writer.write("  netaddress: 127.0.0.1\r\n");
        writer.flush();
        writer.close();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        plainAccessResourceTwo.setRequestCode(203);
        authenticationResult = plainPermissionLoader.eachCheckAuthentication(plainAccessResourceTwo);
        Assert.assertTrue(authenticationResult.isSucceed());

        transport.delete();
        file.delete();
        file = new File("src/test/resources/watch");
        file.delete();

    }

    @Test
    public void analysisTest() {
        BrokerAccessControl accessControl = new BrokerAccessControl();
        accessControl.setSendMessage(false);
        Map<Integer, Boolean> map = accessContralAnalysis.analysis(accessControl);

        Iterator<Entry<Integer, Boolean>> it = map.entrySet().iterator();
        long num = 0;
        while (it.hasNext()) {
            Entry<Integer, Boolean> e = it.next();
            if (!e.getValue()) {
                if (adminCode.contains(e.getKey())) {
                    continue;
                }
                Assert.assertEquals(e.getKey(), Integer.valueOf(10));
                num++;
            }
        }
        Assert.assertEquals(num, 1);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void analysisExceptionTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        accessContralAnalysis.analysis(plainAccessResource);
    }
}
