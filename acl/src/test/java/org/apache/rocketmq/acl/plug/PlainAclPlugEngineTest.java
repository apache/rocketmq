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
package org.apache.rocketmq.acl.plug;

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
import org.apache.rocketmq.acl.plug.PlainAclPlugEngine.AccessContralAnalysis;
import org.apache.rocketmq.acl.plug.PlainAclPlugEngine.BrokerAccessControlTransport;
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

    AccessControl accessControl;

    AccessControl accessControlTwo;

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

        accessControl = new BrokerAccessControl();
        accessControl.setAccount("rokcetmq");
        accessControl.setPassword("aliyun11");
        accessControl.setNetaddress("127.0.0.1");
        accessControl.setRecognition("127.0.0.1:1");

        accessControlTwo = new BrokerAccessControl();
        accessControlTwo.setAccount("rokcet1");
        accessControlTwo.setPassword("aliyun1");
        accessControlTwo.setNetaddress("127.0.0.1");
        accessControlTwo.setRecognition("127.0.0.1:2");

    }

    @Test(expected = AclPlugRuntimeException.class)
    public void accountNullTest() {
        accessControl.setAccount(null);
        plainAclPlugEngine.setAccessControl(accessControl);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void accountThanTest() {
        accessControl.setAccount("123");
        plainAclPlugEngine.setAccessControl(accessControl);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void passWordtNullTest() {
        accessControl.setAccount(null);
        plainAclPlugEngine.setAccessControl(accessControl);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void passWordThanTest() {
        accessControl.setAccount("123");
        plainAclPlugEngine.setAccessControl(accessControl);
    }

    @Test(expected = AclPlugRuntimeException.class)
    public void testPlainAclPlugEngineInit() {
        System.setProperty("rocketmq.home.dir", "");
        new PlainAclPlugEngine().initialize();
    }

    @Test
    public void authenticationInfoOfSetAccessControl() {
        plainAclPlugEngine.setAccessControl(accessControl);

        AuthenticationInfo authenticationInfo = plainAclPlugEngine.getAccessControl(accessControl);

        AccessControl getAccessControl = authenticationInfo.getAccessControl();
        Assert.assertEquals(accessControl, getAccessControl);

        AccessControl testAccessControl = new AccessControl();
        testAccessControl.setAccount("rokcetmq");
        testAccessControl.setPassword("aliyun11");
        testAccessControl.setNetaddress("127.0.0.1");
        testAccessControl.setRecognition("127.0.0.1:1");

        testAccessControl.setAccount("rokcetmq1");
        authenticationInfo = plainAclPlugEngine.getAccessControl(testAccessControl);
        Assert.assertNull(authenticationInfo);

        testAccessControl.setAccount("rokcetmq");
        testAccessControl.setPassword("1234567");
        authenticationInfo = plainAclPlugEngine.getAccessControl(testAccessControl);
        Assert.assertNull(authenticationInfo);

        testAccessControl.setNetaddress("127.0.0.2");
        authenticationInfo = plainAclPlugEngine.getAccessControl(testAccessControl);
        Assert.assertNull(authenticationInfo);
    }

    @Test
    public void setAccessControlList() {
        List<AccessControl> accessControlList = new ArrayList<>();
        accessControlList.add(accessControl);

        accessControlList.add(accessControlTwo);

        plainAclPlugEngine.setAccessControlList(accessControlList);

        AuthenticationInfo newAccessControl = plainAclPlugEngine.getAccessControl(accessControl);
        Assert.assertEquals(accessControl, newAccessControl.getAccessControl());

        newAccessControl = plainAclPlugEngine.getAccessControl(accessControlTwo);
        Assert.assertEquals(accessControlTwo, newAccessControl.getAccessControl());

    }

    @Test
    public void setNetaddressAccessControl() {
        AccessControl accessControl = new BrokerAccessControl();
        accessControl.setAccount("RocketMQ");
        accessControl.setPassword("RocketMQ");
        accessControl.setNetaddress("127.0.0.1");
        plainAclPlugEngine.setAccessControl(accessControl);
        plainAclPlugEngine.setNetaddressAccessControl(accessControl);

        AuthenticationInfo authenticationInfo = plainAclPlugEngine.getAccessControl(accessControl);

        AccessControl getAccessControl = authenticationInfo.getAccessControl();
        Assert.assertEquals(accessControl, getAccessControl);

        accessControl.setNetaddress("127.0.0.2");
        authenticationInfo = plainAclPlugEngine.getAccessControl(accessControl);
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
        list.add((BrokerAccessControl) this.accessControlTwo);
        accessControlTransport.setOnlyNetAddress((BrokerAccessControl) this.accessControl);
        accessControlTransport.setList(list);
        plainAclPlugEngine.setBrokerAccessControlTransport(accessControlTransport);

        AccessControl accessControl = new BrokerAccessControl();
        accessControl.setAccount("RocketMQ");
        accessControl.setPassword("RocketMQ");
        accessControl.setNetaddress("127.0.0.1");
        plainAclPlugEngine.setAccessControl(accessControl);
        AuthenticationInfo authenticationInfo = plainAclPlugEngine.getAccessControl(accessControl);
        Assert.assertNotNull(authenticationInfo.getAccessControl());

        authenticationInfo = plainAclPlugEngine.getAccessControl(accessControlTwo);
        Assert.assertEquals(accessControlTwo, authenticationInfo.getAccessControl());

    }

    @Test
    public void authenticationTest() {
        AuthenticationResult authenticationResult = new AuthenticationResult();
        accessControl.setCode(317);

        boolean isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setCode(321);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        accessControl.setCode(10);
        accessControl.setTopic("permitSendTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setCode(310);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setCode(320);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setTopic("noPermitSendTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        accessControl.setTopic("nopermitSendTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        accessControl.setCode(11);
        accessControl.setTopic("permitPullTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setTopic("noPermitPullTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        accessControl.setTopic("nopermitPullTopic");
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

    }

    @Test
    public void isEmptyTest() {
        AuthenticationResult authenticationResult = new AuthenticationResult();
        accessControl.setCode(10);
        accessControl.setTopic("absentTopic");
        boolean isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        Set<String> permitSendTopic = new HashSet<>();
        brokerAccessControl.setPermitSendTopic(permitSendTopic);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setCode(11);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        brokerAccessControl.setPermitPullTopic(permitSendTopic);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);
    }

    @Test
    public void adminBrokerAccessControlTest() {
        BrokerAccessControl admin = new BrokerAccessControl();
        admin.setAccount("adminTest");
        admin.setPassword("adminTest");
        admin.setNetaddress("127.0.0.1");
        plainAclPlugEngine.setAccessControl(admin);
        Assert.assertFalse(admin.isUpdateAndCreateTopic());

        admin.setAdmin(true);
        plainAclPlugEngine.setAccessControl(admin);
        Assert.assertTrue(admin.isUpdateAndCreateTopic());
    }

    @Test
    public void adminEachCheckAuthentication() {
        BrokerAccessControl accessControl = new BrokerAccessControl();
        accessControl.setAccount("RocketMQ1");
        accessControl.setPassword("1234567");
        accessControl.setNetaddress("127.0.0.1");
        plainAclPlugEngine.setAccessControl(accessControl);
        for (Integer code : adminCode) {
            accessControl.setCode(code);
            AuthenticationResult authenticationResult = plainAclPlugEngine.eachCheckAuthentication(accessControl);
            Assert.assertFalse(authenticationResult.isSucceed());

        }
        plainAclPlugEngine.cleanAuthenticationInfo();
        accessControl.setAdmin(true);
        plainAclPlugEngine.setAccessControl(accessControl);
        for (Integer code : adminCode) {
            accessControl.setCode(code);
            AuthenticationResult authenticationResult = plainAclPlugEngine.eachCheckAuthentication(accessControl);
            Assert.assertTrue(authenticationResult.isSucceed());
        }
    }

    @Test
    public void cleanAuthenticationInfoTest() {
        plainAclPlugEngine.setAccessControl(accessControl);
        accessControl.setCode(202);
        AuthenticationResult authenticationResult = plainAclPlugEngine.eachCheckAuthentication(accessControl);
        Assert.assertTrue(authenticationResult.isSucceed());
        plainAclPlugEngine.cleanAuthenticationInfo();
        authenticationResult = plainAclPlugEngine.eachCheckAuthentication(accessControl);
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
        accessControl.setCode(203);
        AuthenticationResult authenticationResult = plainAclPlugEngine.eachCheckAuthentication(accessControl);
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
        accessControlTwo.setCode(203);
        authenticationResult = plainAclPlugEngine.eachCheckAuthentication(accessControlTwo);
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
        AccessControl accessControl = new AccessControl();
        accessContralAnalysis.analysis(accessControl);
    }
}
