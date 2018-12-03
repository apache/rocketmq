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
import org.apache.rocketmq.acl.plain.PlainAclPlugEngine.AccessContralAnalysis;
import org.apache.rocketmq.acl.plain.PlainAclPlugEngine.BrokerAccessControlTransport;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PlainAclPlugEngineTest {

    PlainAclPlugEngine plainAclPlugEngine;

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

        authenticationInfo = new AuthenticationInfo(map, brokerAccessControl, NetaddressStrategyFactory.NULL_NET_ADDRESS_STRATEGY);

        System.setProperty("rocketmq.home.dir", "src/test/resources");
        plainAclPlugEngine = new PlainAclPlugEngine();

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
        plainAclPlugEngine.setAccessControl(plainAccessResource);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void accountThanTest() {
        plainAccessResource.setAccessKey("123");
        plainAclPlugEngine.setAccessControl(plainAccessResource);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void passWordtNullTest() {
        plainAccessResource.setAccessKey(null);
        plainAclPlugEngine.setAccessControl(plainAccessResource);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void passWordThanTest() {
        plainAccessResource.setAccessKey("123");
        plainAclPlugEngine.setAccessControl(plainAccessResource);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void testPlainAclPlugEngineInit() {
        System.setProperty("rocketmq.home.dir", "");
        new PlainAclPlugEngine().initialize();
    }

    @Test
    public void authenticationInfoOfSetAccessControl() {
        plainAclPlugEngine.setAccessControl(plainAccessResource);

        AuthenticationInfo authenticationInfo = plainAclPlugEngine.getAccessControl(plainAccessResource);

        PlainAccessResource getPlainAccessResource = authenticationInfo.getPlainAccessResource();
        Assert.assertEquals(plainAccessResource, getPlainAccessResource);

        PlainAccessResource testPlainAccessResource = new PlainAccessResource();
        testPlainAccessResource.setAccessKey("rokcetmq");
        testPlainAccessResource.setSignature("aliyun11");
        testPlainAccessResource.setRemoteAddr("127.0.0.1");
        testPlainAccessResource.setRecognition("127.0.0.1:1");

        testPlainAccessResource.setAccessKey("rokcetmq1");
        authenticationInfo = plainAclPlugEngine.getAccessControl(testPlainAccessResource);
        Assert.assertNull(authenticationInfo);

        testPlainAccessResource.setAccessKey("rokcetmq");
        testPlainAccessResource.setSignature("1234567");
        authenticationInfo = plainAclPlugEngine.getAccessControl(testPlainAccessResource);
        Assert.assertNull(authenticationInfo);

        testPlainAccessResource.setRemoteAddr("127.0.0.2");
        authenticationInfo = plainAclPlugEngine.getAccessControl(testPlainAccessResource);
        Assert.assertNull(authenticationInfo);
    }

    @Test
    public void setAccessControlList() {
        List<PlainAccessResource> plainAccessResourceList = new ArrayList<>();
        plainAccessResourceList.add(plainAccessResource);

        plainAccessResourceList.add(plainAccessResourceTwo);

        plainAclPlugEngine.setAccessControlList(plainAccessResourceList);

        AuthenticationInfo newAccessControl = plainAclPlugEngine.getAccessControl(plainAccessResource);
        Assert.assertEquals(plainAccessResource, newAccessControl.getPlainAccessResource());

        newAccessControl = plainAclPlugEngine.getAccessControl(plainAccessResourceTwo);
        Assert.assertEquals(plainAccessResourceTwo, newAccessControl.getPlainAccessResource());

    }

    @Test
    public void setNetaddressAccessControl() {
        PlainAccessResource plainAccessResource = new BrokerAccessControl();
        plainAccessResource.setAccessKey("RocketMQ");
        plainAccessResource.setSignature("RocketMQ");
        plainAccessResource.setRemoteAddr("127.0.0.1");
        plainAclPlugEngine.setAccessControl(plainAccessResource);
        plainAclPlugEngine.setNetaddressAccessControl(plainAccessResource);

        AuthenticationInfo authenticationInfo = plainAclPlugEngine.getAccessControl(plainAccessResource);

        PlainAccessResource getPlainAccessResource = authenticationInfo.getPlainAccessResource();
        Assert.assertEquals(plainAccessResource, getPlainAccessResource);

        plainAccessResource.setRemoteAddr("127.0.0.2");
        authenticationInfo = plainAclPlugEngine.getAccessControl(plainAccessResource);
        Assert.assertNull(authenticationInfo);
    }

    public void eachCheckLoginAndAuthentication() {

    }

    @Test(expected = AclPlugRuntimeException.class)
    public void BrokerAccessControlTransportTestNull() {
        BrokerAccessControlTransport accessControlTransport = new BrokerAccessControlTransport();
        plainAclPlugEngine.setBrokerAccessControlTransport(accessControlTransport);
    }

    @Test
    public void BrokerAccessControlTransportTest() {
        BrokerAccessControlTransport accessControlTransport = new BrokerAccessControlTransport();
        List<BrokerAccessControl> list = new ArrayList<>();
        list.add((BrokerAccessControl) this.plainAccessResourceTwo);
        accessControlTransport.setOnlyNetAddress((BrokerAccessControl) this.plainAccessResource);
        accessControlTransport.setList(list);
        plainAclPlugEngine.setBrokerAccessControlTransport(accessControlTransport);

        PlainAccessResource plainAccessResource = new BrokerAccessControl();
        plainAccessResource.setAccessKey("RocketMQ");
        plainAccessResource.setSignature("RocketMQ");
        plainAccessResource.setRemoteAddr("127.0.0.1");
        plainAclPlugEngine.setAccessControl(plainAccessResource);
        AuthenticationInfo authenticationInfo = plainAclPlugEngine.getAccessControl(plainAccessResource);
        Assert.assertNotNull(authenticationInfo.getPlainAccessResource());

        authenticationInfo = plainAclPlugEngine.getAccessControl(plainAccessResourceTwo);
        Assert.assertEquals(plainAccessResourceTwo, authenticationInfo.getPlainAccessResource());

    }

    @Test
    public void authenticationTest() {
        AuthenticationResult authenticationResult = new AuthenticationResult();
        plainAccessResource.setRequestCode(317);

        boolean isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setRequestCode(321);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        plainAccessResource.setRequestCode(10);
        plainAccessResource.setTopic("permitSendTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setRequestCode(310);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setRequestCode(320);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setTopic("noPermitSendTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        plainAccessResource.setTopic("nopermitSendTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        plainAccessResource.setRequestCode(11);
        plainAccessResource.setTopic("permitPullTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setTopic("noPermitPullTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        plainAccessResource.setTopic("nopermitPullTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

    }

    @Test
    public void isEmptyTest() {
        AuthenticationResult authenticationResult = new AuthenticationResult();
        plainAccessResource.setRequestCode(10);
        plainAccessResource.setTopic("absentTopic");
        boolean isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        Set<String> permitSendTopic = new HashSet<>();
        brokerAccessControl.setPermitSendTopic(permitSendTopic);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);

        plainAccessResource.setRequestCode(11);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertFalse(isReturn);

        brokerAccessControl.setPermitPullTopic(permitSendTopic);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, plainAccessResource, authenticationResult);
        Assert.assertTrue(isReturn);
    }

    @Test
    public void adminBrokerAccessControlTest() {
        BrokerAccessControl admin = new BrokerAccessControl();
        admin.setAccessKey("adminTest");
        admin.setSignature("adminTest");
        admin.setRemoteAddr("127.0.0.1");
        plainAclPlugEngine.setAccessControl(admin);
        Assert.assertFalse(admin.isUpdateAndCreateTopic());

        admin.setAdmin(true);
        plainAclPlugEngine.setAccessControl(admin);
        Assert.assertTrue(admin.isUpdateAndCreateTopic());
    }

    @Test
    public void adminEachCheckAuthentication() {
        BrokerAccessControl accessControl = new BrokerAccessControl();
        accessControl.setAccessKey("RocketMQ1");
        accessControl.setSignature("1234567");
        accessControl.setRemoteAddr("127.0.0.1");
        plainAclPlugEngine.setAccessControl(accessControl);
        for (Integer code : adminCode) {
            accessControl.setRequestCode(code);
            AuthenticationResult authenticationResult = plainAclPlugEngine.eachCheckAuthentication(accessControl);
            Assert.assertFalse(authenticationResult.isSucceed());

        }
        plainAclPlugEngine.cleanAuthenticationInfo();
        accessControl.setAdmin(true);
        plainAclPlugEngine.setAccessControl(accessControl);
        for (Integer code : adminCode) {
            accessControl.setRequestCode(code);
            AuthenticationResult authenticationResult = plainAclPlugEngine.eachCheckAuthentication(accessControl);
            Assert.assertTrue(authenticationResult.isSucceed());
        }
    }

    @Test
    public void cleanAuthenticationInfoTest() {
        plainAclPlugEngine.setAccessControl(plainAccessResource);
        plainAccessResource.setRequestCode(202);
        AuthenticationResult authenticationResult = plainAclPlugEngine.eachCheckAuthentication(plainAccessResource);
        Assert.assertTrue(authenticationResult.isSucceed());
        plainAclPlugEngine.cleanAuthenticationInfo();
        authenticationResult = plainAclPlugEngine.eachCheckAuthentication(plainAccessResource);
        Assert.assertFalse(authenticationResult.isSucceed());
    }

    @Test
    public void isWatchStartTest() {
        PlainAclPlugEngine plainAclPlugEngine = new PlainAclPlugEngine();
        Assert.assertTrue(plainAclPlugEngine.isWatchStart());
        System.setProperty("java.version", "1.6.11");
        plainAclPlugEngine = new PlainAclPlugEngine();
        Assert.assertFalse(plainAclPlugEngine.isWatchStart());
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
        PlainAclPlugEngine plainAclPlugEngine = new PlainAclPlugEngine();
        plainAccessResource.setRequestCode(203);
        AuthenticationResult authenticationResult = plainAclPlugEngine.eachCheckAuthentication(plainAccessResource);
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
        authenticationResult = plainAclPlugEngine.eachCheckAuthentication(plainAccessResourceTwo);
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
