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

    BrokerAccessControl BrokerAccessControl;

    @Before
    public void init() throws NoSuchFieldException, SecurityException, IOException {

        accessContralAnalysis.analysisClass(RequestCode.class);

        BrokerAccessControl = new BrokerAccessControl();
        // 321
        BrokerAccessControl.setQueryConsumeQueue(false);

        Set<String> permitSendTopic = new HashSet<>();
        permitSendTopic.add("permitSendTopic");
        BrokerAccessControl.setPermitSendTopic(permitSendTopic);

        Set<String> noPermitSendTopic = new HashSet<>();
        noPermitSendTopic.add("noPermitSendTopic");
        BrokerAccessControl.setNoPermitSendTopic(noPermitSendTopic);

        Set<String> permitPullTopic = new HashSet<>();
        permitPullTopic.add("permitPullTopic");
        BrokerAccessControl.setPermitPullTopic(permitPullTopic);

        Set<String> noPermitPullTopic = new HashSet<>();
        noPermitPullTopic.add("noPermitPullTopic");
        BrokerAccessControl.setNoPermitPullTopic(noPermitPullTopic);

        AccessContralAnalysis accessContralAnalysis = new AccessContralAnalysis();
        accessContralAnalysis.analysisClass(RequestCode.class);
        Map<Integer, Boolean> map = accessContralAnalysis.analysis(BrokerAccessControl);

        authenticationInfo = new AuthenticationInfo(map, BrokerAccessControl, NetaddressStrategyFactory.NULL_NET_ADDRESS_STRATEGY);

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
        BrokerAccessControl.setPermitSendTopic(permitSendTopic);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setCode(11);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        BrokerAccessControl.setPermitPullTopic(permitSendTopic);
        isReturn = plainAclPlugEngine.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);
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
